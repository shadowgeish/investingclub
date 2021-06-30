# Check if a begginning date is falls the reserved slots

def check_day_slot(start_date, slots_list, meeting_end_date, type='datetime'):
    no_free_slot = 0

    # print('check_day_slot : checking slot list = {} for {}'.format(slots_list, start_date))

    for slot in slots_list:
        start_check_date = slot['start_date_time'] if type == 'datetime' else datetime.datetime(start_date.year,
                                                                                                start_date.month,
                                                                                                start_date.day,
                                                                                                int(slot[
                                                                                                        'start_time'].split(
                                                                                                    "h")[0]),
                                                                                                int(slot[
                                                                                                        'start_time'].split(
                                                                                                    "h")[1])
                                                                                                )

        end_check_date = slot['end_date_time'] if type == 'datetime' else datetime.datetime(start_date.year,
                                                                                            start_date.month,
                                                                                            start_date.day,
                                                                                            int(slot['end_time'].split(
                                                                                                "h")[0]),
                                                                                            int(slot['end_time'].split(
                                                                                                "h")[1])
                                                                                            )

        # print('Slot {}: start_check_date = {}, end_check_date = {}'.format(slot, start_check_date, end_check_date))

        if start_check_date < start_date < end_check_date or start_check_date < meeting_end_date < end_check_date:
            # print('! Reserved Slot {}: start_check_date = {}, end_check_date = {} for {}'.format(slot,
            #                                                                                     start_check_date,
            #                                                                                     end_check_date,
            #                                                                                     start_date))
            return end_check_date

    # print('! Not reserved check_day_slot : checking slot list = {} for {}'.format(slots_list, start_date))
    return start_date


def round_date(date, rd=15):
    minute = date.minute
    if (minute % rd) != 0:
        target_minutes = (int(minute / rd) + 1) * rd
        add_minutes = target_minutes - minute
        date = date + datetime.timedelta(minutes=add_minutes)

    return date


def get_next_five_day_time_slot(meeting_type, start_date, holidays, delay_to_book_meeting,
                                meeting_type_duration_table,
                                week_settings_table, book_slot_table):
    meeting_type_duration = 30

    if meeting_type in meeting_type_duration_table.keys():
        meeting_type_duration = meeting_type_duration_table[meeting_type]

    print('Duration for each meeting is {}'.format(meeting_type_duration))

    # print('start date to display meeting is {}'.format(start_date))

    # 0 = Monday, 6 = Sunday
    # print('Current day of the week is {}'.format(start_date.weekday()))

    # date_list = [start_date + datetime.timedelta(days=x) for x in range(7)]
    # print('current list of day for slots {}'.format(date_list))
    date_slot_list = {}
    map_weekday_number = {0: "Monday", 1: "Tuesday", 2: "Wednesday", 3: "Thursday", 4: "Friday", 5: "Saturday",
                          6: "Sunday"}

    # for each day,
    # for date in date_list:
    day_count = 1
    date = start_date
    pivot_datetime = start_date
    slots = []
    while day_count < 7:

        print('COUNT {} Starting on day {} which is a {}'.format(day_count, date, map_weekday_number[date.weekday()]))

        week_dat_config = week_settings_table[map_weekday_number[date.weekday()]]

        sod = datetime.datetime(date.year, date.month, date.day,
                                hour=int(week_dat_config['start_hour'].split("h")[0]),
                                minute=int(week_dat_config['start_hour'].split("h")[1]))

        eod = datetime.datetime(date.year, date.month, date.day,
                                hour=int(week_dat_config['end_hour'].split("h")[0]),
                                minute=int(week_dat_config['end_hour'].split("h")[1]))

        # check if the datetime is withing the working hour of the week
        print(' DAY Start and End eod = {} for {}'.format(sod, eod))
        if date < sod:
            print(' TOO early Check working hours: eod = {} for {}'.format(sod, date))
            date = sod

        if date != sod:
            date = date + datetime.timedelta(minutes=delay_to_book_meeting)

        date = round_date(date)

        # check if day is holiday
        for holiday in holidays:
            dd = datetime.date(date.year, date.month, date.day)
            if holiday['start_date'] < dd and holiday['end_date'] > dd:
                day_count = day_count + 1
                date = date + datetime.timedelta(1)
                continue

        while date < eod:

            meeting_end_date = date + datetime.timedelta(minutes=meeting_type_duration)
            # meeting_end_date_and_delay = meeting_end_date + datetime.timedelta(minutes=delay_to_book_meeting)

            if meeting_end_date > eod:
                date = eod
                continue
            breaks = week_dat_config['breaks']

            check_reserved_slot = check_day_slot(date, book_slot_table, meeting_end_date, type='datetime')
            check_break_slot = check_day_slot(date, breaks, meeting_end_date, type='time')
            print('Check slots for date = {}, check_reserved_slot = {} check_break_slot = {}'.format(date,
                                                                                                     check_reserved_slot,
                                                                                                     check_break_slot))
            if check_reserved_slot == date and check_break_slot == date:
                slots.append(date)
                date = meeting_end_date
            else:
                date = max(check_reserved_slot, check_break_slot)

        date = sod + datetime.timedelta(1)
        day_count = day_count + 1

        # get beginning slots (start + delay)
    print('Slotsssss = {}'.format(slots))
    # check if beginning is not in a blocked slots (breaks)


if __name__ == '__main__':
    # get_asset_returns(asset_codes=["BX4.PA", "CAC.PA"])
    import pandas as pd
    import numpy as np
    import datetime

    tday = datetime.date.today()
    start_date = (tday + datetime.timedelta(100))
    end_date = (start_date + datetime.timedelta(15))
    print('holidays : start = {}, end = {}'.format(start_date, end_date))

    holidays = [
        {
            "start_date": start_date,
            "end_date": end_date
        }
    ]

    # advisors_unavailability : advisor_id, start_date_time, end_date_time, unavailability_type (holiday, other)

    # meeting_type : advisor_id, name, duration, description

    # week_settings : advisor_id, week_day, is_day_off, start_hour, end_hour, serialized_breaks [start_time, end_time]

    delay_to_book_meeting = 60

    meeting_type_duration_table = {"first Appointment": 45, "Follow up meeting": 60, "test meeting": 15}

    tate = (tday + datetime.timedelta(1))
    bs1 = datetime.datetime(tate.year, tate.month, tate.day, hour=9, minute=30)
    es1 = datetime.datetime(tate.year, tate.month, tate.day, hour=10, minute=30)

    tate2 = (tday + datetime.timedelta(3))
    bs2 = datetime.datetime(tate.year, tate.month, tate.day, hour=14, minute=30)
    es2 = datetime.datetime(tate.year, tate.month, tate.day, hour=15, minute=30)

    book_slot_table = [{
        "start_date_time": bs1,
        "end_date_time": es1
    },
        {
            "start_date_time": bs2,
            "end_date_time": es2
        }]

    week_settings_table = {
        "Monday": {
            "day_off": 0,
            "start_hour": "08h00",
            "end_hour": "18h00",
            "between_meeting_break": "10",  # min
            "breaks": [{
                "start_time": "12h30",
                "end_time": "13h30"
            },
                {
                    "start_time": "14h30",
                    "end_time": "15h00"
                }]
        },

        "Tuesday": {
            "day_off": 0,
            "start_hour": "08h00",
            "end_hour": "18h00",
            "between_meeting_break": "10",  # min
            "breaks": [{
                "start_time": "12h30",
                "end_time": "13h30"
            },
                {
                    "start_time": "14h30",
                    "end_time": "15h00"
                }]
        },

        "Wednesday": {
            "day_off": 0,
            "start_hour": "08h00",
            "end_hour": "18h00",
            "between_meeting_break": "10",  # min
            "breaks": [{
                "start_time": "12h30",
                "end_time": "13h30"
            },
                {
                    "start_time": "14h30",
                    "end_time": "15h00"
                }]
        },
        "Thursday": {
            "day_off": 0,
            "start_hour": "08h00",
            "end_hour": "18h00",
            "between_meeting_break": "10",  # min
            "breaks": [{
                "start_time": "12h30",
                "end_time": "13h30"
            },
                {
                    "start_time": "14h30",
                    "end_time": "15h00"
                }]
        },
        "Friday": {
            "day_off": 0,
            "start_hour": "08h00",
            "end_hour": "18h00",
            "between_meeting_break": "10",  # min
            "breaks": [{
                "start_time": "12h30",
                "end_time": "13h30"
            },
                {
                    "start_time": "14h30",
                    "end_time": "15h00"
                }]
        },
        "Saturday": {
            "day_off": 0,
            "start_hour": "08h00",
            "end_hour": "18h00",
            "between_meeting_break": "10",  # min
            "breaks": [{
                "start_time": "12h30",
                "end_time": "13h30"
            },
                {
                    "start_time": "14h30",
                    "end_time": "15h00"
                }]
        },
        "Sunday": {
            "day_off": 1,
            "start_hour": "08h00",
            "end_hour": "18h00",
            "between_meeting_break": "10",  # min
            "breaks": [{
                "start_time": "12h30",
                "end_time": "13h30"
            },
                {
                    "start_time": "14h30",
                    "end_time": "15h00"
                }]
        }
    }

    get_next_five_day_time_slot("first Appointment", datetime.datetime.now(), holidays, delay_to_book_meeting,
                                meeting_type_duration_table,
                                week_settings_table, book_slot_table)
