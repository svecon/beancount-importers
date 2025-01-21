import csv
from datetime import datetime

from beancount.core.data import Transaction
from beancount.core.data import Posting
from beancount.core import data
from beancount.core.amount import Amount
from beancount.ingest import importer
from beancount.core.number import Decimal

import pdb


class InvalidFormatError(Exception):
    pass


def fmt_number_de(value: str) -> Decimal:
    thousands_sep = "."
    decimal_sep = ","

    return Decimal(value.replace(thousands_sep, "").replace(decimal_sep, "."))


def DecimalOrZero(value):
    # for string to number conversion with empty strings
    try:
        return Decimal(value)
    except:
        return Decimal(0.0)


class BcgeImporter(importer.ImporterProtocol):
    def __init__(self, account, currency="CHF", file_encoding="cp1252", manual_fixes=0):

        self.account = account
        self.currency = currency
        self.file_encoding = file_encoding
        self.language = ""

        self._date_from = None
        self._date_to = None
        self._balance_amount = None
        self._balance_date = None
        self.delimiter = ";"
        self.manual_fixes = manual_fixes

    def name(self):
        return "Bcge {}".format(self.__class__.__name__)

    def file_account(self, file):
        return self.account

    def file_date(self, file):
        return datetime.now

    def identify(self, file):
        return "bcge" in file.name and file.name.endswith(".csv")

    def extract(self, file, existing_entries=None):
        entries = []
        with open(file.name, encoding=self.file_encoding) as fd:

            reader = csv.reader(fd, delimiter=self.delimiter)

            # Skip 11 lines
            for _ in range(11):
                next(reader)

            header = next(reader)
            print(header)

            amount_index = header.index("Amount")
            date_index = header.index("Date")
            description_index = header.index("Posting text")

            # Data entries
            for i, row in enumerate(reader):
                if len(row) == 0:  # "end" of bank statment
                    break

                meta = data.new_metadata(file.name, i)
                date = datetime.strptime(row[date_index], "%d.%m.%y").date()
                amount = Amount(DecimalOrZero(row[amount_index]), self.currency)
                description = row[description_index]

                trans = Transaction(
                    meta,
                    date,
                    self.FLAG,
                    "",
                    description,
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    [],
                )
                trans.postings.append(
                    Posting(self.account, amount, None, None, None, None)
                )
                trans.postings.append(
                    Posting("Expenses:TBD", -amount, None, None, None, None)
                )
                entries.append(trans)

        return entries
