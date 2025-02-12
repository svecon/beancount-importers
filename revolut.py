from dateutil.parser import parse
from datetime import timedelta, datetime
from io import StringIO

from beancount.ingest import importer
from beancount.core import data
from beancount.core import amount
from beancount.core.number import D

import csv


class RevolutImporter(importer.ImporterProtocol):
    """An importer for Revolut CSV files."""

    def __init__(self, account, currency="CHF", file_encoding="utf-8", manual_fixes=0):

        self.account = account
        self.currency = currency
        self.file_encoding = file_encoding
        self.language = ""

    def name(self):
        return "Revolut {}".format(self.__class__.__name__)

    def file_account(self, file):
        return self.account

    def file_date(self, file):
        return datetime.now

    def identify(self, file):
        return "revolut" in file.name

    def extract(self, file, existing_entries):
        entries = []
        has_balance = False

        with StringIO(file.contents()) as csvfile:
            reader = csv.DictReader(
                csvfile,
                [
                    "Type",
                    "Product",
                    "Started Date",
                    "Completed Date",
                    "Description",
                    "Amount",
                    "Fee",
                    "Currency",
                    "State",
                    "Balance",
                ],
                delimiter=",",
                skipinitialspace=True,
            )
            next(reader)
            for row in reader:

                if row["State"] != "COMPLETED":
                    continue

                book_date = parse(row["Started Date"].split()[0].strip()).date()

                amt = amount.Amount(D(row["Amount"]), row["Currency"])

                meta = data.new_metadata(
                    file.name,
                    0,
                    {"balance": data.Amount(D(row["Balance"]), row["Currency"])},
                )
                entry = data.Transaction(
                    meta,
                    book_date,
                    "*",
                    "",
                    row["Description"].strip(),
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    [
                        data.Posting(self.account, amt, None, None, None, None),
                        data.Posting(
                            "Expenses:TBD",
                            -amt,
                            None,
                            None,
                            None,
                            None,
                        ),
                    ],
                )
                if D(row["Fee"]) != 0:
                    fee = amount.Amount(D(row["Fee"]), row["Currency"])
                    entry.postings.append(
                        data.Posting(
                            "Expenses:Financial:Fees",
                            fee,
                            None,
                            None,
                            None,
                            None,
                        )
                    )
                entries.append(entry)

        return entries
