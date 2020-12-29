#!/usr/bin/env python
import sys
from dataclasses import dataclass
from datetime import datetime, date
import csv, re
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
    lineno: int

    @classmethod
    def fromRow(cls, row, lineno):
        try:
            transaction_date = datetime.strptime(row[0], "%d/%m/%Y")
        except ValueError:
            return None

        if len(row[3].strip()) != 0:
            delta = row[3].strip()
            is_withdraw = True
        else:
            delta = row[4].strip()
            is_withdraw = False
        return cls(
            transaction_date=datetime.strptime(row[0], "%d/%m/%Y").date(),
            fop=row[2],
            description=row[2].strip(),
            value_delta=Decimal(delta.replace(",", "")),
            is_withdraw=is_withdraw,
            lineno=lineno,
        )

    def appendDescription(self, row):
        self.description = row[2]


def scan_file(opened_file) -> List[OcbcTransaction]:
    latest_txn = None  # Date, description, value delta, is withdraw
    parse_results = []
    line_index = 0
    for s in csv.reader(opened_file):
        lineno = line_index
        line_index += 1
        if len(s) < 3:
            continue
        if latest_txn != None:
            if len(s[0]) == 0:
                latest_txn.appendDescription(s)
                continue
            else:
                parse_results.append(latest_txn)

        latest_txn = OcbcTransaction.fromRow(s, lineno)
    if latest_txn != None:
        parse_results.append(latest_txn)
    return parse_results


# return (txn_date, digested_description)
def guess_txn_date(ocbc_txn: OcbcTransaction):
    txn_date = ocbc_txn.transaction_date
    description = ocbc_txn.description
    if ocbc_txn.fop == "DEBIT PURCHASE":
        match = re.match(r"^(\d\d/\d\d/\d\d) (.*)$", ocbc_txn.description)
        if match != None:
            txn_date = datetime.strptime(match.group(1), "%d/%m/%y").date()
            description = match.group(2)
        else:
            match = re.match(r"^(.*) (\d\d/\d\d/\d\d)$", ocbc_txn.description)
            if match != None:
                txn_date = datetime.strptime(match.group(2), "%d/%m/%y").date()
                description = match.group(1)
    return (txn_date, description.replace("\t", " ").strip())


class Importer(importer.ImporterProtocol):
    """An importer for OCBC CSV files."""

    def __init__(self, account_cash):
        self.account_cash = account_cash

    def name(self):
        return "OCBC CSV Importer"

    def identify(self, file):
        return re.match(r".*TransactionHistory_.*csv", file.name) != None

    def extract(self, file):
        ledger_txns = []
        self.filename = file.name
        with open(file.name) as infile:
            ocbc_txns = scan_file(infile)
            for ocbc_txn in ocbc_txns:
                ledger_txns.append(self.process_txn(ocbc_txn))
        return ledger_txns

    def process_txn(self, txn):
        meta = data.new_metadata(self.filename, txn.lineno)
        (txn_date, ocbc_desc) = guess_txn_date(txn)
        meta.update({"ocbc_desc": ocbc_desc, "fop": txn.fop})
        if txn_date != txn.transaction_date:
            meta.update({"aux_date": txn.transaction_date})

        for rule_name in self.expansion_rules:
            postings = getattr(self, rule_name)(txn)
            category = rule_name.replace("_", " ").capitalize()
            if postings != None:
                return data.Transaction(
                    meta,
                    txn_date,
                    self.FLAG,
                    None,
                    category,
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    postings,
                )
        raise ValueError("Invalid txn:" + ocbc_txn)

    def init_cost_posting(
        self, ocbc_txn: OcbcTransaction, dest_account: str, value_delta: int = None
    ) -> data.Posting:
        if value_delta == None:
            value_delta = ocbc_txn.value_delta
            if not ocbc_txn.is_withdraw:
                value_delta = -ocbc_txn.value_delta
        return data.Posting(
            dest_account, amount.Amount(value_delta, "SGD"), None, None, None, {}
        )

    def init_outgoing_posting(self, ocbc_txn: OcbcTransaction) -> data.Posting:
        value_delta = ocbc_txn.value_delta
        if not ocbc_txn.is_withdraw:
            value_delta = -ocbc_txn.value_delta
        return data.Posting(
            self.account_cash, amount.Amount(-value_delta, "SGD"), None, None, None, {}
        )

    def init_postings(self, txn: OcbcTransaction, dest_account) -> List[data.Posting]:
        return [
            self.init_cost_posting(txn, dest_account),
            self.init_outgoing_posting(txn),
        ]

    def init_postings_multi_dest(
        self, txn: OcbcTransaction, dest_account_amount_map
    ) -> List[data.Posting]:
        postings = []
        for (dest_account, value_delta) in dest_account_amount_map.items():
            postings.append(self.init_cost_posting(txn, dest_account, value_delta))
        postings.append(self.init_outgoing_posting(txn))
        return postings

    def bus(self, ocbc_txn):
        if ocbc_txn.is_withdraw and re.search(r"BUS/MRT", ocbc_txn.description) != None:
            return self.init_postings(ocbc_txn, "Expenses:Transit:Public")
        return None

    def bubbletea(self, ocbc_txn):
        if ocbc_txn.is_withdraw and (
            re.search(r"KOI", ocbc_txn.description) != None
            or re.search(r"CHICHA SAN CHEN", ocbc_txn.description) != None
            or re.search(r"HEYTEA", ocbc_txn.description) != None
        ):
            return self.init_postings(ocbc_txn, "Expenses:Drink")
        return None

    def dining(self, ocbc_txn):
        if (
            ocbc_txn.is_withdraw
            and (
                re.search(r"YIHE", ocbc_txn.description) != None
                or re.search(r"KOUFU PTE", ocbc_txn.description) != None
                or ocbc_txn.value_delta > Decimal(30)
            )
            and ocbc_txn.value_delta < Decimal(300)
        ):
            return self.init_postings(ocbc_txn, "Expenses:Food")
        return None

    def tax(self, ocbc_txn):
        if (
            ocbc_txn.is_withdraw
            and re.search(r"IRAS TAXS", ocbc_txn.description) != None
        ):
            return self.init_postings(ocbc_txn, "Expenses:Tax")
        return None

    def tv(self, ocbc_txn):
        if (
            ocbc_txn.is_withdraw
            and re.search(r"YOUTUBEPREMIUM", ocbc_txn.description) != None
        ):
            return self.init_postings_multi_dest(
                ocbc_txn,
                {
                    "Expenses:Utils:Tv": Decimal("5.98"),
                    "Assets:Debt:YtFamily": Decimal("12"),
                },
            )
        if ocbc_txn.is_withdraw and (
            re.search(r"Netflix", ocbc_txn.description) != None
            or re.search(r"NETFLIX", ocbc_txn.description) != None
        ):
            return self.init_postings(ocbc_txn, "Expenses:Utils:Tv")
        return None

    def rent(self, ocbc_txn):
        if ocbc_txn.is_withdraw and re.search(r"Rent to", ocbc_txn.description) != None:
            return self.init_postings(ocbc_txn, "Expenses:Rent")

    def utils(self, ocbc_txn):
        if (
            ocbc_txn.is_withdraw
            and re.search(r"SP SERVICES", ocbc_txn.description) != None
        ):
            return self.init_postings(ocbc_txn, "Expenses:Utils")

    def invest(self, ocbc_txn):
        if (
            ocbc_txn.is_withdraw
            and re.search(r"RoboInvest", ocbc_txn.description) != None
        ):
            return self.init_postings(ocbc_txn, "Assets:Investment:Ocbc:Roboinvest")

        if ocbc_txn.is_withdraw and (
            re.search(r"ASIA WEALTH PLAT", ocbc_txn.description) != None
            or re.search(r"Asia Wealth Plat", ocbc_txn.description) != None
        ):
            return self.init_postings(ocbc_txn, "Assets:Stashaway")

    def basic_income(self, ocbc_txn):
        if not ocbc_txn.is_withdraw and (
            re.search(r"SALARY.*GOOGLE", ocbc_txn.description) != None
            or re.search(r"SALARY", ocbc_txn.fop) != None
        ):
            return self.init_postings(ocbc_txn, "Income:Salary")
        if not ocbc_txn.is_withdraw and (
            re.search(r"INTEREST", ocbc_txn.fop) != None
            or re.search(r"INTEREST", ocbc_txn.description) != None
        ):
            return self.init_postings(ocbc_txn, "Income:Bank:Ocbc:Interest")
        return None

    def gpay_reward(self, ocbc_txn):
        if (
            not ocbc_txn.is_withdraw
            and re.search(r"OTHR GOOGLE PAY MENT", ocbc_txn.description) != None
        ):
            return self.init_postings(ocbc_txn, "Income:Cashback:GPay")
        return None

    def default(self, ocbc_txn):
        if ocbc_txn.is_withdraw:
            return self.init_postings(ocbc_txn, "Expenses:Misc")
        else:
            return self.init_postings(ocbc_txn, "Income:Misc")

    expansion_rules = [
        "utils",
        "bus",
        "bubbletea",
        "tax",
        "rent",
        "tv",
        "invest",
        "basic_income",
        "gpay_reward",
        "dining",
        "default",
    ]
