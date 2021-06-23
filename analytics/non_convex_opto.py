from pypfopt import EfficientFrontier
from pypfopt import objective_functions, base_optimizer
import pandas as pd
import numpy as np
import warnings
import cvxpy as cp

class MaxDiversification(base_optimizer.BaseConvexOptimizer):

    def __init__(
            self,
            expected_returns,
            cov_matrix,
            weight_bounds=(0, 1),
            solver=None,
            verbose=False,
            solver_options=None,
    ):

        # Inputs
        self.cov_matrix = MaxDiversification._validate_cov_matrix(cov_matrix)
        self.expected_returns = MaxDiversification._validate_expected_returns(
            expected_returns
        )
        print("expected_returns {}".format(self.expected_returns))

        # Labels
        if isinstance(expected_returns, pd.Series):
            tickers = list(expected_returns.index)
        elif isinstance(cov_matrix, pd.DataFrame):
            tickers = list(cov_matrix.columns)
        else:  # use integer labels
            tickers = list(range(len(expected_returns)))

        if expected_returns is not None and cov_matrix is not None:
            if cov_matrix.shape != (len(expected_returns), len(expected_returns)):
                raise ValueError("Covariance matrix does not match expected returns")

        super().__init__(
            len(tickers),
            tickers,
            weight_bounds,
            solver=solver,
            verbose=verbose,
            solver_options=solver_options,
        )

    @staticmethod
    def _validate_expected_returns(expected_returns):
        if expected_returns is None:
            return None
        elif isinstance(expected_returns, pd.Series):
            return expected_returns.values
        elif isinstance(expected_returns, list):
            return np.array(expected_returns)
        elif isinstance(expected_returns, np.ndarray):
            return expected_returns.ravel()
        else:
            raise TypeError("expected_returns is not a series, list or array")

    @staticmethod
    def _validate_cov_matrix(cov_matrix):
        if cov_matrix is None:
            raise ValueError("cov_matrix must be provided")
        elif isinstance(cov_matrix, pd.DataFrame):
            return cov_matrix.values
        elif isinstance(cov_matrix, np.ndarray):
            return cov_matrix
        else:
            raise TypeError("cov_matrix is not a dataframe or array")

    def _validate_returns(self, returns):
        """
        Helper method to validate daily returns (needed for some efficient frontiers)
        """
        if not isinstance(returns, (pd.DataFrame, np.ndarray)):
            raise TypeError("returns should be a pd.Dataframe or np.ndarray")

        returns_df = pd.DataFrame(returns)
        if returns_df.isnull().values.any():
            warnings.warn(
                "Removing NaNs from returns",
                UserWarning,
            )
            returns_df = returns_df.dropna(axis=0, how="any")

        if self.expected_returns is not None:
            if returns_df.shape[1] != len(self.expected_returns):
                raise ValueError(
                    "returns columns do not match expected_returns. Please check your tickers."
                )

        return returns_df

    def _make_weight_sum_constraint(self, is_market_neutral):
        """
        Helper method to make the weight sum constraint. If market neutral,
        validate the weights proided in the constructor.
        """
        if is_market_neutral:
            #  Check and fix bounds
            portfolio_possible = np.any(self._lower_bounds < 0)
            if not portfolio_possible:
                warnings.warn(
                    "Market neutrality requires shorting - bounds have been amended",
                    RuntimeWarning,
                )
                self._map_bounds_to_constraints((-1, 1))
                # Delete original constraints
                del self._constraints[0]
                del self._constraints[0]

            self._constraints.append(cp.sum(self._w) == 0)
        else:
            self._constraints.append(cp.sum(self._w) == 1)

    def optimize(self, long_only = True):
        import cvxpy as cp
        import numpy as np
        """
        Minimise volatility.

        :return: asset weights for the volatility-minimising portfolio
        :rtype: OrderedDict
        """
        #self._objective = self.diversification_ratio()
        #objective_functions.portfolio_variance(
        #    self._w, self.cov_matrix
        #)

        # Market-neutral efficient risk
        # bnd: individual position limit
        # long only: long only constraint
        #constraints = ({"type": "eq", "fun": lambda w: 1 - np.sum(w)},)
        #if long_only:  # add in long only constraint
        #    constraints = constraints + ({'type': 'ineq', 'fun': lambda w: np.sum(w)},)

        constraints = [
            {"type": "eq", "fun": lambda w: 0.5-w[3]}  # weights sum to zero
        ]

        return self.nonconvex_objective(
            custom_objective=self.diversification_ratio,  # min negative return (i.e maximise return)
            objective_args=(self.cov_matrix,),
            weights_sum_to_one=True,
            #constraints=constraints,
            solver='SLSQP'
        )

    @staticmethod
    def diversification_ratio(w, cov_matrix):
        print('w={}'.format(w))

        """
        Calculate the total portfolio variance (i.e square volatility).

        :param w: asset weights in the portfolio
        :type w: np.ndarray OR cp.Variable
        :param cov_matrix: covariance matrix
        :type cov_matrix: np.ndarray
        :return: value of the objective function OR objective function expression
        :rtype: float OR cp.Expression
        """

        # average weighted vol
        w_vol = np.dot(np.sqrt(np.diag(cov_matrix)), w.T)
        variance = cp.quad_form(w, cov_matrix)
        diversification_ratio = (w_vol / variance)


        """
            Return either the value of the objective function
            or the objective function as a cvxpy object depending on whether
            w is a cvxpy variable or np array.
        """
        obj_value = None
        if isinstance(w, np.ndarray):
            if np.isscalar(diversification_ratio):
                obj_value = diversification_ratio
            elif np.isscalar(diversification_ratio.value):
                obj_value = diversification_ratio.value
            else:
                obj_value = diversification_ratio.value.item()
        else:
            obj_value = diversification_ratio

        print('obj_value=-{}'.format(obj_value))
        return -obj_value

    def portfolio_performance(self, verbose=False, risk_free_rate=0.02):
        """
        After optimising, calculate (and optionally print) the performance of the optimal
        portfolio. Currently calculates expected return, volatility, and the Sharpe ratio.

        :param verbose: whether performance should be printed, defaults to False
        :type verbose: bool, optional
        :param risk_free_rate: risk-free rate of borrowing/lending, defaults to 0.02.
                               The period of the risk-free rate should correspond to the
                               frequency of expected returns.
        :type risk_free_rate: float, optional
        :raises ValueError: if weights have not been calcualted yet
        :return: expected return, volatility, Sharpe ratio.
        :rtype: (float, float, float)
        """
        if self._risk_free_rate is not None:
            if risk_free_rate != self._risk_free_rate:
                warnings.warn(
                    "The risk_free_rate provided to portfolio_performance is different"
                    " to the one used by max_sharpe. Using the previous value.",
                    UserWarning,
                )
            risk_free_rate = self._risk_free_rate

        return base_optimizer.portfolio_performance(
            self.weights,
            self.expected_returns,
            self.cov_matrix,
            verbose,
            risk_free_rate,
        )