import csv
import re
from datetime import datetime, timedelta

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


class CSOBImporter(importer.ImporterProtocol):
    def __init__(self, account, currency="CZK", file_encoding="UTF-8", manual_fixes=0):

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
        return "CSOB {}".format(self.__class__.__name__)

    def file_account(self, _):
        return self.account

    def file_date(self, file_):
        return datetime.now

    def identify(self, file_):
        return "csob" in file_.name

    def extract(self, file_, existing_entries=None):
        entries = []
        with open(file_.name, encoding=self.file_encoding) as fd:

            reader = csv.reader(fd, delimiter=self.delimiter)

            next(reader)  # skip header line
            next(reader)  # skip header line
            header = next(reader)  # header

            # Data entries
            for i, row in enumerate(reader):
                if len(row) == 0:  # "end" of bank statment
                    break

                date = datetime.strptime(
                    row[header.index("due date")], "%d.%m.%Y"
                ).date()
                amount = Amount(
                    DecimalOrZero(fmt_number_de(row[header.index("amount")])),
                    row[header.index("currency")],
                )
                balance = Amount(
                    DecimalOrZero(fmt_number_de(row[header.index("balance")])),
                    row[header.index("currency")],
                )
                description = row[header.index("note")]
                payee = row[header.index("counter account name")]
                if (
                    not description
                    and row[header.index("counter account")] == "1112003761"
                ):
                    description = "VZP Health Insurance"

                place = ""
                match = re.search("Place: ([A-Za-z ]+) ", description)
                if match:
                    place = match.group(1)

                trans = Transaction(
                    data.new_metadata(file_.name, i, {"balance": balance}),
                    date,
                    self.FLAG,
                    payee,
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
