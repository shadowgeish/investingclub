from datetime import datetime


class Order:
    """ Represents an order inside the orderbook with its current status 
    
    """

    # __slots__ = ["uid", "is_buy", "qty", "price", "timestamp", "status"]

    def __init__(self, uid, is_buy, qty, price, timestamp=datetime.now()):
        self.uid = uid
        self.is_buy = is_buy
        self.qty = qty
        # outstanding volume in orderbook. If filled or canceled => 0. 
        self.leavesqty = qty
        # You should not access _cumqty directly.
        # Use cumqty property instead
        self._cumqty = None
        self.price = price
        self.timestamp = timestamp
        # is the order active and resting in the orderbook?
        self.active = False
        # DDL attributes import unittest
        self.prev = None
        self.next = None

    def __str__(self):
        return "uid={},price={},qty={},leavesqty={},timestamp={}".format(self.uid, self.price, self.qty, self.leavesqty, self.timestamp)

    def order_data(self):

        return {"uid": self.uid, "price": self.price, "qty":self.qty, "leavesqty": self.leavesqty,
                 "timestamp": self.timestamp.strftime("%d%m%Y%H%M%z"), "active": self.active, "cumqty": self.cumqty}

    @property
    def cumqty(self):
        if self._cumqty:
            return self._cumqty
        else:
            return self.qty - self.leavesqty