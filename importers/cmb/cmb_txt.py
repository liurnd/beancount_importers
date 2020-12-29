#!/usr/bin/env python
import csv,re
from decimal import Decimal
from typing import List, Union
from beancount.ingest import importer
from beancount.core import data, account, amount
from enum import Enum
from datetime import datetime, date
import datetime
from dataclasses import dataclass

ParseDirective = Enum('ParseDirective',
                      ['Finalized',
                       'NextRow'])

@dataclass
class CmbTransaction:
    transaction_date: date
    ledger_date: date
    fop: str
    description: str
    value_delta: Decimal
    value_delta_local: Decimal
    local_currency: str
    is_withdraw: bool
    step: int
    year: int

    @classmethod
    def new(cls, year):
        return cls(transaction_date = None,
                   ledger_date = None,
                   fop = '',
                   description = '',
                   local_currency = '',
                   value_delta = None,
                   value_delta_local = None,
                   is_withdraw = False,
                   step = 0,
                   year = year)

    def scan_row(self, row):
        row = row.strip()
        directive = ParseDirective.NextRow

        if self.step == 0:
            self.transaction_date = self.parse_date_str(row)
        elif self.step == 2:
            self.ledger_date = self.parse_date_str(row)
        elif self.step == 4:
            self.description = row
        elif self.step == 6:
            v = row
            self.is_withdraw = (not v[1] == '-')
            self.value_delta = Decimal(row.split(' ')[1].replace(',',''))
        elif self.step == 8:
            self.fop = row
        elif self.step == 10:
            if row != '':
                self.local_currency = row
        elif self.step == 12:
            if row != '':
                self.value_delta_local = Decimal(row.replace(',','')).copy_abs()
        elif self.step == 16:
            directive = ParseDirective.Finalized
        self.step += 1
        return (directive, self)

    def parse_date_str(self, s):
        month = int(s[:2])
        day = int(s[2:])
        return datetime.date(self.year, month, day)

@dataclass
class LedgerTransaction:
    category: str
    transaction_date: date
    ledger_date: date
    fop: str
    description: str
    dest_account: str
    value_delta: Decimal
    value_delta_local: Decimal
    local_currency: str
    is_withdraw: bool

def food(cmb_txn):
    if cmb_txn.is_withdraw and re.search(r"DELIVEROO", cmb_txn.description)!=None:
        return [init_ledger_txn_by_cmb(cmb_txn, dest_account = 'Expenses:Food')]
    return None

def grocery(cmb_txn):
    if cmb_txn.is_withdraw and (re.search(r"NTUC", cmb_txn.description)!=None or
                                re.search(r"COLD STORAGE", cmb_txn.description)!=None or
                                re.search(r"Redmart", cmb_txn.description)!=None):
        return [init_ledger_txn_by_cmb(cmb_txn, dest_account = 'Expenses:Groceries')]
    return None

def grab(cmb_txn):
    if cmb_txn.is_withdraw and re.search(r"Grab Grab*", cmb_txn.description)!=None and cmb_txn.value_delta > Decimal(400) and cmb_txn.value_delta < Decimal(600) :
        ledger_txn = init_ledger_txn_by_cmb(cmb_txn, dest_account = 'Assets:Wallet:Grab', category = 'Grab Recharge')

        return []
    if cmb_txn.is_withdraw and re.search(r"Grab Grab*", cmb_txn.description)!=None:
        return [init_ledger_txn_by_cmb(cmb_txn, dest_account = 'Expenses:Transit')]

def cashback(cmb_txn):
    if cmb_txn.is_withdraw and re.search(r"Grab Grab*", cmb_txn.description)!=None and cmb_txn.value_delta > Decimal(400) and cmb_txn.value_delta < Decimal(600) :
        ledger_txn = init_ledger_txn_by_cmb(cmb_txn, dest_account = 'Assets:Wallet:Grab', category = 'Grab Recharge')

        return []
    if cmb_txn.is_withdraw and re.search(r"Grab Grab*", cmb_txn.description)!=None:
        return [init_ledger_txn_by_cmb(cmb_txn, dest_account = 'Expenses:Transit')]


def default_converter(cmb_txn):
    if cmb_txn.is_withdraw:
        return [init_ledger_txn_by_cmb(cmb_txn, dest_account = 'Expenses:Misc')]
    else:
        return [init_ledger_txn_by_cmb(cmb_txn, dest_account = 'Income:Misc')]

expansion_rules = [grab, food, grocery, default_converter]

def init_ledger_txn_by_cmb(cmb_txn: CmbTransaction, dest_account = '', category = 'Day') -> LedgerTransaction:
    local_currency = ''
    if cmb_txn.delta_value_local != None and cmb_txn.delta_value != cmb_txn.delta_value_local:
        if cmb_txn.local_currency == 'SG':
            local_currency = 'SGD'
        elif cmb_txn.local_currency == 'US':
            local_currency = 'USD'

    return LedgerTransaction(category = category,
                             transaction_date = cmb_txn.transaction_date,
                             ledger_date = cmb_txn.ledger_date,
                             description = cmb_txn.description,
                             dest_account = dest_account,
                             value_delta = cmb_txn.value_delta,
                             value_delta_local = cmb_txn.value_delta_local,
                             local_currency = local_currency,
                             is_withdraw = cmb_txn.is_withdraw,
                             fop = cmb_txn.fop)

def process_txn(cmb_txn: CmbTransaction) -> List[LedgerTransaction]:
    for rule in expansion_rules:
        results = rule(cmb_txn)
        if not results == None:
            return results
    raise ValueError("Invalid txn:" + cmb_txn)

def scan_file(infile, year) -> List[CmbTransaction]:
    txn = CmbTransaction.new(year)
    txns = []
    for l in infile:
        (directive, txn) = txn.scan_row(l)
        if directive == ParseDirective.Finalized:
            txns.append(txn)
            txn = CmbTransaction.new(year)
    if txn.step != 0:
        txns.append(txn)
    return txns

class Importer(importer.ImporterProtocol):
    """An importer for CMB Email files."""

    def __init__(self, fop_account_map, account_grab, year):
        self.fop_account_map = fop_account_map
        self.account_grab = account_grab
        self.year = year

    def name(self):
        return "CMB Email Importer"

    def identify(self, file):
        return re.match(r'.*cmb.*txt', file.name) != None

    def extract(self, file):
        ledger_txns = []
        with open(file.name) as infile:
            cmb_txns = scan_file(infile, self.year)
            for cmb_txn in cmb_txns:
                ledger_txns +=  process_txn(cmb_txn = cmb_txn)
        return self.generate_beancount_data_entries(ledger_txns, file.name)

    def generate_beancount_data_entries(self, ledger_txns: List[LedgerTransaction], filename) -> List[data.Transaction]:
        ledger_entries_map = {}
        for txn in ledger_txns:
            key = (txn.transaction_date, txn.ledger_date, txn.category, txn.fop)
            if not key in ledger_entries_map:
                ledger_entries_map[key] = []
                ledger_entries_map[key].append(txn)

        result = []
        index = 0
        for key in sorted(ledger_entries_map, key=lambda k: (k[1], k[0], k[2], k[3])):
            (txn_date, ledger_date, category, fop) = key
            postings = []
            meta = data.new_metadata(filename, index)

            if (txn_date != ledger_date):
                meta.update({'aux_date': ledger_date})

            for txn in ledger_entries_map[key]:
                txn_amount = txn.value_delta
                if not txn.is_withdraw:
                    txn_amount = -txn_amount
                postings.append(
                    data.Posting(txn.dest_account, amount.Amount(txn_amount, 'RMB'), None,  None, None,
                                 {'cmb_desc': txn.description}))

            postings.append(data.Posting(self.fop_account_map[fop], None, None, None, None,
                                             None))

            result.append(data.Transaction(
                            meta,
                            txn_date,
                            self.FLAG,
                            None,
                            category,
                            data.EMPTY_SET,
                            data.EMPTY_SET,
                            postings))
            index += 1
        return result
