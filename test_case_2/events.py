import time

'''Event-driven approach helps to maintain more clear software design
and better control what is going on inside the system.

Basic types of events: Info, Trade, Error.

    Info provides service info messages like 'you are connected'.

    Trade contains trade data.

    Error provides error messages like 'no data'.
'''
class Event:
    pass

class InfoEvent(Event):
    def __init__(self, message:str):
        self.type = 'InfoEvent'
        self.timestamp = time.time()
        self.data = message

class TradeEvent(Event):
    def __init__(self, trades:list):
        self.type = 'TradeEvent'
        self.timestamp = time.time()
        self.data = trades

class ErrorEvent(Event):
    def __init__(self, message:str):
        self.type = 'ErrorEvent'
        self.timestamp = time.time()
        self.data = message
