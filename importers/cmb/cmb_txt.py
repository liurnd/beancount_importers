#!/usr/bin/env python
import csv,re
from decimal import Decimal
from typing import List, Union
from beancount.ingest import importer
from beancount.core import data, account, amount
from beancount.core.position import CostSpec
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
    lineno: int

    @classmethod
    def new(cls, year, lineno):
        return cls(transaction_date = None,
                   ledger_date = None,
                   fop = '',
                   description = '',
                   local_currency = '',
                   value_delta = None,
                   value_delta_local = None,
                   is_withdraw = False,
                   step = 0,
                   year = year,
                   lineno = lineno)

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
        self.filename = file.name
        with open(file.name) as infile:
            cmb_txns = self.scan_file(infile)
            for cmb_txn in cmb_txns:
                ledger_txns.append(self.process_txn(cmb_txn))
            return ledger_txns

    def scan_file(self, infile) -> List[CmbTransaction]:
        lineno = 0
        txn = CmbTransaction.new(self.year, lineno)
        txns = []

        for l in infile:
            (directive, txn) = txn.scan_row(l)
            if directive == ParseDirective.Finalized:
                txns.append(txn)
                txn = CmbTransaction.new(self.year, lineno)
            lineno += 1
        if txn.step != 0:
            txns.append(txn)
        return txns

    def init_cost_posting_by_cmb_txn(self, cmb_txn: CmbTransaction, dest_account: str) -> data.Posting:
        local_currency = 'RMB'
        cost_spec = None
        if cmb_txn.value_delta_local != None and cmb_txn.value_delta != cmb_txn.value_delta_local:
            local_currency = 'SGD'
            if cmb_txn.local_currency == 'US':
                local_currency = 'USD'
            cost_spec = CostSpec(number_per = None,
                                 number_total = cmb_txn.value_delta,
                                 date = None,
                                 label = None,
                                 merge = None,
                                 currency = 'RMB')
        value_delta = cmb_txn.value_delta_local
        if not cmb_txn.is_withdraw:
            value_delta = -cmb_txn.value_delta_local

        return data.Posting(dest_account,
                     amount.Amount(value_delta, local_currency),
                     cost_spec,
                     None,
                     None,
                     {'cmb_desc': cmb_txn.description})


    def init_outgoing_posting_by_cmb_txn(self, cmb_txn: CmbTransaction) -> data.Posting:
        txn_amount = None
        value_delta = cmb_txn.value_delta
        if not cmb_txn.is_withdraw:
            value_delta = -cmb_txn.value_delta

        return data.Posting(self.fop_account_map[cmb_txn.fop],
                     amount.Amount(-value_delta, 'RMB'),
                     None,
                     None,
                     None,
                     {})

    def init_postings_by_cmb_txn(self, cmb_txn: CmbTransaction, dest_account) -> List[data.Posting]:
        return [
            self.init_cost_posting_by_cmb_txn(cmb_txn, dest_account),
            self.init_outgoing_posting_by_cmb_txn(cmb_txn),
        ]

    def food(self, cmb_txn):
        if cmb_txn.is_withdraw and re.search(r"DELIVEROO", cmb_txn.description)!=None:
            return self.init_postings_by_cmb_txn(cmb_txn, 'Expenses:Food')
        return None

    def grocery(self, cmb_txn):
        if cmb_txn.is_withdraw and (re.search(r"NTUC", cmb_txn.description)!=None or
                                    re.search(r"DOOKKI", cmb_txn.description)!=None or
                                    re.search(r"WATSON", cmb_txn.description)!=None or
                                    re.search(r"COLD STORAGE", cmb_txn.description)!=None or
                                    re.search(r"Redmart", cmb_txn.description)!=None):
            return self.init_postings_by_cmb_txn(cmb_txn, 'Expenses:Groceries')
        return None

    def grab(self, cmb_txn):
        if cmb_txn.is_withdraw and re.search(r"Grab Grab*", cmb_txn.description)!=None and cmb_txn.value_delta > Decimal(400) and cmb_txn.value_delta < Decimal(600):
            cost_posting = self.init_cost_posting_by_cmb_txn(cmb_txn, 'Assets:Wallet:Grab')
            cost_posting = cost_posting._replace(
                units = amount.Amount(Decimal(100), 'SGD'),
                cost = CostSpec(
                    number_per = None,
                    number_total = cmb_txn.value_delta,
                    date = None,
                    label = None,
                    merge = None,
                    currency = 'RMB')
            )
            return [
                cost_posting,
                self.init_outgoing_posting_by_cmb_txn(cmb_txn),
                ]
        if cmb_txn.is_withdraw and re.search(r"Grab Grab*", cmb_txn.description)!=None:
            return self.init_postings_by_cmb_txn(cmb_txn, 'Expenses:Transit')

    def cashback(self, cmb_txn):
        if not cmb_txn.is_withdraw and (re.search(r"返现", cmb_txn.description)!=None or
                                        re.search(r"红包", cmb_txn.description)!=None):
            return self.init_postings_by_cmb_txn(cmb_txn, 'Income:Cashback:Cmb0933')
        return None

    def utility(self, cmb_txn):
        if cmb_txn.is_withdraw and re.search(r"SP DIGITAL", cmb_txn.description)!=None:
            return self.init_postings_by_cmb_txn(cmb_txn, 'Expenses:Utils')
        return None

    def default_converter(self, cmb_txn):
        if cmb_txn.is_withdraw:
            return [
                self.init_cost_posting_by_cmb_txn(cmb_txn, 'Expenses:Misc'),
                self.init_outgoing_posting_by_cmb_txn(cmb_txn),
            ]
        else:
            return [
                self.init_cost_posting_by_cmb_txn(cmb_txn, 'Income:Misc'),
                self.init_outgoing_posting_by_cmb_txn(cmb_txn),
            ]

    expansion_rules = ['food', 'grocery', 'grab', 'cashback', 'utility', 'default_converter']

    def process_txn(self, cmb_txn):
        meta = data.new_metadata(self.filename, cmb_txn.lineno)
        if (cmb_txn.transaction_date != cmb_txn.ledger_date):
            meta.update({'aux_date': cmb_txn.ledger_date})

        for rule_name in self.expansion_rules:
            postings = getattr(self, rule_name)(cmb_txn)
            category = rule_name.replace('_', ' ').capitalize()
            if postings != None:
                return data.Transaction(
                    meta,
                    cmb_txn.transaction_date,
                    self.FLAG,
                    None,
                    category,
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    postings)
        raise ValueError("Invalid txn:" + ocbc_txn)
