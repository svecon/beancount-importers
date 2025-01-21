from dateutil.parser import parse
from datetime import timedelta, datetime
from io import StringIO

from beancount.ingest import importer
from beancount.core import data
from beancount.core import amount
from beancount.core.number import D

import csv


class WiseImporter(importer.ImporterProtocol):
    """An importer for Wise CSV files."""

    def __init__(self, account, currency="CHF", file_encoding="utf-8", manual_fixes=0):

        self.account = account
        self.currency = currency
        self.file_encoding = file_encoding
        self.language = ""

    def name(self):
        return "Wise {}".format(self.__class__.__name__)

    def file_account(self, file):
        return self.account

    def file_date(self, file):
        return datetime.now

    def identify(self, file):
        return "wise" in file.name

    def extract(self, file, existing_entries):
        entries = []

        with StringIO(file.contents()) as csvfile:
            reader = csv.DictReader(
                csvfile,
                [
                    "TransferWise ID",
                    "Date",
                    "Amount",
                    "Currency",
                    "Description",
                    "Payment Reference",
                    "Running Balance",
                    "Exchange From",
                    "Exchange To",
                    "Exchange Rate",
                    "Payer Name",
                    "Payee Name",
                    "Payee Account Number",
                    "Merchant",
                    "Card Last Four Digits",
                    "Card Holder Full Name",
                    "Attachment",
                    "Note",
                    "Total fees",
                    "Exchange To Amount",
                ],
                delimiter=",",
                skipinitialspace=True,
            )
            next(reader)
            for row in reader:

                book_date = parse(row["Date"]).date()

                amt = amount.Amount(D(row["Amount"]), row["Currency"])
                description = row["Description"].strip()
                if "issued by" in description:
                    description = description.split("issued by")[1].strip()

                meta = data.new_metadata(
                    file.name,
                    0,
                    {
                        "balance": data.Amount(
                            D(row["Running Balance"]), row["Currency"]
                        )
                    },
                )
                entry = data.Transaction(
                    meta,
                    book_date,
                    "*",
                    "",
                    description,
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
                if D(row["Total fees"]) != 0:
                    fee = amount.Amount(D(row["Total fees"]), row["Currency"])
                    entry.postings.append(
                        data.Posting(
                            self.account,
                            -fee,
                            None,
                            None,
                            None,
                            None,
                        )
                    )
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
