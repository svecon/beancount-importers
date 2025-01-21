import csv
from datetime import datetime

from beancount.core.data import Balance
from beancount.core.data import Transaction
from beancount.core.data import Posting
from beancount.core import data
from beancount.core.amount import Amount
from beancount.ingest import importer
from beancount.core.number import Decimal


class InvalidFormatError(Exception):
    pass


def fmt_number_de(value: str) -> Decimal:
    thousands_sep = "."
    decimal_sep = ","

    return Decimal(value.replace(thousands_sep, "").replace(decimal_sep, "."))


def DecimalOrZero(value):
    # for string to number conversion with empty strings
    try:
        return Decimal(value.replace(",", "."))
    except:
        return Decimal(0.0)


class FioImporter(importer.ImporterProtocol):
    def __init__(
        self, account, currency="CZK", file_encoding="utf-8-sig", manual_fixes=0
    ):

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

        self.tags = {}

    def name(self):
        return "Fio {}".format(self.__class__.__name__)

    def file_account(self, file):
        return self.account

    def file_date(self, file):
        return datetime.now

    def identify(self, file):
        return "Vypis_z_uctu-2000466790" in file.name

    def extract(self, file_, existing_entries=None):
        entries = []
        with open(file_.name, encoding=self.file_encoding) as fd:
            reader = csv.reader(fd, delimiter=self.delimiter)

            # Skip 12 lines
            for _ in range(9):
                line = next(reader)
            #     if len(line) == 2 and "openingBalance" in line[0]:
            #         opening_balance = line[1].replace(" ", "")
            #     if len(line) == 2 and "dateStart" in line[0]:
            #         opening_date = datetime.strptime(line[1], "%d.%m.%Y").date()
            #     if len(line) == 2 and "closingBalance" in line[0]:
            #         closing_balance = line[1].replace(" ", "")
            #     if len(line) == 2 and "dateEnd" in line[0]:
            #         closing_date = datetime.strptime(line[1], "%d.%m.%Y").date()
            #
            # meta = data.new_metadata(file_.name, 0)
            # entries.append(
            #     Balance(
            #         meta,
            #         opening_date,
            #         "Assets:CZ:Fio:Personal",
            #         Amount(DecimalOrZero(opening_balance), self.currency),
            #         Decimal(0.0),
            #         Amount(Decimal(0.0), self.currency),
            #     )
            # )
            #
            header = next(reader)  # skip header line
            # print(header)
            amount_index = header.index("Objem")
            date_index = header.index("Datum")
            payee_index = header.index("Název protiúčtu")
            description_index = header.index("Poznámka")
            provedl_index = header.index("Provedl")

            # Data entries
            for i, row in enumerate(reader):
                if len(row) == 0:  # "end" of bank statement
                    break

                meta = data.new_metadata(file_.name, i)
                date = datetime.strptime(row[date_index], "%d.%m.%Y").date()
                amount = Amount(DecimalOrZero(row[amount_index]), self.currency)
                payee = row[payee_index]
                description = row[description_index]
                provedl = row[provedl_index]

                if not description:
                    description = payee

                if not description:
                    description = provedl

                if "Nákup: " in description:
                    description = (
                        description.replace("Nákup: ", "").split(",", 1)[0].strip()
                    )

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

            meta = data.new_metadata(file_.name, 0)
            # entries.append(
            #     Balance(
            #         meta,
            #         closing_date,
            #         "Assets:CZ:Fio:Personal",
            #         Amount(DecimalOrZero(closing_balance), self.currency),
            #         Decimal(0.0),
            #         Amount(Decimal(0.0), self.currency),
            #     )
            # )

        return entries
