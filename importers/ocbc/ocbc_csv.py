#!/usr/bin/env python
import sys
from dataclasses import dataclass
from datetime import datetime, date
import csv,re
from decimal import Decimal
from typing import List
from beancount.ingest import importer
from beancount.core import data, account, amount


@dataclass
class OcbcTransaction:
    transaction_date: date
    fop: str
    description: str
    value_delta: Decimal
    is_withdraw: bool

    @classmethod
    def fromRow(cls, row):
        try:
            transaction_date = datetime.strptime(row[0], "%d/%m/%Y")
        except ValueError:
            return None

        if len(row[3].strip())!=0:
            delta = row[3].strip()
            is_withdraw = True
        else:
            delta = row[4].strip()
            is_withdraw = False
        return cls(transaction_date = datetime.strptime(row[0], "%d/%m/%Y").date(),
                   fop = row[2],
                   description = row[2].strip(),
                   value_delta = Decimal(delta.replace(',', '')),
                   is_withdraw = is_withdraw)

    def appendDescription(self, row):
        self.description = row[2]

def scan_file(opened_file) -> List[OcbcTransaction]:
    latest_txn = None # Date, description, value delta, is withdraw
    parse_results = []
    for s in csv.reader(opened_file):
        if (len(s) < 3):
            continue
        if (latest_txn != None):
            if (len(s[0]) == 0):
                latest_txn.appendDescription(s)
                continue
            else:
                parse_results.append(latest_txn)

        latest_txn = OcbcTransaction.fromRow(s)
    if latest_txn != None:
        parse_results.append(latest_txn)
    return parse_results


@dataclass
class LedgerTransaction:
    category: str
    transaction_date: date
    ocbc_ledger_date: date
    fop: str
    description: str
    dest_account: str
    value_delta: Decimal
    is_withdraw: bool

def bus(ocbc_txn):
    if ocbc_txn.is_withdraw and re.search(r"BUS/MRT", ocbc_txn.description)!=None:
        return [init_ledger_txn_by_ocbc(ocbc_txn, dest_account = 'Expenses:Transit')]
    return None

def bubbletea_converter(ocbc_txn):
    if ocbc_txn.is_withdraw and (
        re.search(r"KOI", ocbc_txn.description)!=None or
        re.search(r"CHICHA SAN CHEN", ocbc_txn.description)!=None or
        re.search(r"HEYTEA", ocbc_txn.description)!=None):
        return [init_ledger_txn_by_ocbc(ocbc_txn, dest_account = 'Expenses:Drink')]
    return None

def dining(ocbc_txn):
    if ocbc_txn.is_withdraw and (
        re.search(r"YIHE", ocbc_txn.description)!=None or
        re.search(r"KOUFU PTE", ocbc_txn.description)!=None or
        ocbc_txn.value_delta > Decimal(30)) and ocbc_txn.value_delta < Decimal(300):
        return [init_ledger_txn_by_ocbc(ocbc_txn, dest_account = 'Expenses:Food')]
    return None

def tax(ocbc_txn):
    if ocbc_txn.is_withdraw and re.search(r"IRAS TAXS", ocbc_txn.description)!=None:
        return [init_ledger_txn_by_ocbc(ocbc_txn, dest_account = 'Expenses:Tax', category = 'Tax')]
    return None

def tv(ocbc_txn):
    if ocbc_txn.is_withdraw and re.search(r"YOUTUBEPREMIUM", ocbc_txn.description)!=None:
        expense_txn = init_ledger_txn_by_ocbc(ocbc_txn, dest_account = 'Expenses:Utils:Tv')
        expense_txn.value_delta = Decimal('5.98')
        debt_txn = init_ledger_txn_by_ocbc(ocbc_txn, dest_account = 'Assets:Debt:YtFamily')
        debt_txn.value_delta = Decimal('12')
        return [expense_txn, debt_txn]
    if ocbc_txn.is_withdraw and re.search(r"Netflix", ocbc_txn.description)!=None:
        return [init_ledger_txn_by_ocbc(ocbc_txn, dest_account = 'Expenses:Utils:Tv')]
    return None

def rent(ocbc_txn):
    if ocbc_txn.is_withdraw and re.search(r"Rent to", ocbc_txn.description)!=None:
        return [init_ledger_txn_by_ocbc(ocbc_txn, dest_account = 'Expenses:Rent', category = 'Rent')]

def invest(ocbc_txn):
    if ocbc_txn.is_withdraw and re.search(r"RoboInvest", ocbc_txn.description)!=None:
        return [init_ledger_txn_by_ocbc(ocbc_txn, dest_account = 'Assets:Investment:Ocbc:Roboinvest', category = 'Investment')]
    if ocbc_txn.is_withdraw and (re.search(r"ASIA WEALTH PLAT", ocbc_txn.description)!=None or
                                 re.search(r"Asia Wealth Plat", ocbc_txn.description)!=None
                                 ):
        return [init_ledger_txn_by_ocbc(ocbc_txn, dest_account = 'Assets:Stashaway', category = 'Investment')]

def basic_income(ocbc_txn):
    if not ocbc_txn.is_withdraw and (
        re.search(r"SALARY.*GOOGLE", ocbc_txn.description)!=None or
        re.search(r"SALARY", ocbc_txn.fop)!=None):
        return [init_ledger_txn_by_ocbc(ocbc_txn, dest_account = 'Income:Salary', category = 'Salary')]
    if not ocbc_txn.is_withdraw and re.search(r"INTEREST", ocbc_txn.fop)!=None:
        return [init_ledger_txn_by_ocbc(ocbc_txn, dest_account = 'Income:Bank:Ocbc:Interest', category = 'Interest')]
    if not ocbc_txn.is_withdraw and re.search(r"INTEREST", ocbc_txn.description)!=None:
        return [init_ledger_txn_by_ocbc(ocbc_txn, dest_account = 'Income:Bank:Ocbc:Interest', category = 'Interest')]
    return None

def gpay_reward(ocbc_txn):
    if not ocbc_txn.is_withdraw and re.search(r"OTHR GOOGLE PAY MENT", ocbc_txn.description)!=None:
        return [init_ledger_txn_by_ocbc(ocbc_txn, dest_account = 'Income:Cashback:GPay', category = 'Cashback')]
    return None

def default_converter(ocbc_txn):
    if ocbc_txn.is_withdraw:
        return [init_ledger_txn_by_ocbc(ocbc_txn, dest_account = 'Expenses:Misc')]
    else:
        return [init_ledger_txn_by_ocbc(ocbc_txn, dest_account = 'Income:Misc')]

expansion_rules = [bus, bubbletea_converter, tax, rent, tv, invest, basic_income, gpay_reward, dining, default_converter]

def process_txn(ocbc_txn: OcbcTransaction) -> List[LedgerTransaction]:
    for rule in expansion_rules:
        results = rule(ocbc_txn)
        if not results == None:
            return results
    raise ValueError("Invalid txn:" + ocbc_txn)

def init_ledger_txn_by_ocbc(ocbc_txn: OcbcTransaction, dest_account = '', category = 'Day') -> LedgerTransaction:
    (txn_date, description) = guess_txn_date(ocbc_txn)
    return LedgerTransaction(category = category,
                             transaction_date = txn_date,
                             ocbc_ledger_date = ocbc_txn.transaction_date,
                             description = description,
                             dest_account = dest_account,
                             value_delta = ocbc_txn.value_delta,
                             is_withdraw = ocbc_txn.is_withdraw,
                             fop = ocbc_txn.fop)

# return (txn_date, digested_description)
def guess_txn_date(ocbc_txn: OcbcTransaction):
    txn_date = ocbc_txn.transaction_date
    description = ocbc_txn.description
    if ocbc_txn.fop == 'DEBIT PURCHASE':
        match = re.match(r'^(\d\d/\d\d/\d\d) (.*)$', ocbc_txn.description)
        if match != None:
            txn_date = datetime.strptime(match.group(1), '%d/%m/%y').date()
            description = match.group(2)
        else:
            match = re.match(r'^(.*) (\d\d/\d\d/\d\d)$', ocbc_txn.description)
            if match != None:
                txn_date = datetime.strptime(match.group(2), '%d/%m/%y').date()
                description = match.group(1)
    return (txn_date, description.replace('\t', ' ').strip())

class Importer(importer.ImporterProtocol):
    """An importer for OCBC CSV files."""

    def __init__(self, account_cash):
        self.account_cash = account_cash

    def name(self):
        return "OCBC CSV Importer"
    def identify(self, file):
        return True

    def extract(self, file):
        ledger_txns = []
        with open(file.name) as infile:
            ocbc_txns = scan_file(infile)
            for ocbc_txn in ocbc_txns:
                ledger_txns +=  process_txn(ocbc_txn = ocbc_txn)
        return self.generate_beancount_data_entries(ledger_txns, file.name)

    def generate_beancount_data_entries(self, ledger_txns: List[LedgerTransaction], filename) -> List[data.Transaction]:
        ledger_entries_map = {}
        for txn in ledger_txns:
            key = (txn.transaction_date, txn.ocbc_ledger_date, txn.category)
            if not key in ledger_entries_map:
                ledger_entries_map[key] = []
                ledger_entries_map[key].append(txn)

        result = []
        index = 0
        for key in sorted(ledger_entries_map, key=lambda k: (k[1], k[0], k[2])):
            (txn_date, ledger_date, category) = key
            postings = []
            meta = data.new_metadata(filename, index)

            if (txn_date != ledger_date):
                meta.update({'aux_date': ledger_date})

            for txn in ledger_entries_map[key]:
                txn_amount = txn.value_delta
                if not txn.is_withdraw:
                    txn_amount = -txn_amount
                postings.append(
                    data.Posting(txn.dest_account, amount.Amount(txn_amount, 'SGD'), None,  None, None,
                                 {'ocbc_desc': txn.description,
                                  'fop': txn.fop}))

            postings.append(data.Posting(self.account_cash, None, None, None, None,
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
