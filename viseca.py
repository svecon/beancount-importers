import json
from datetime import datetime

from beancount.core.data import Transaction
from beancount.core.data import Posting
from beancount.core import data
from beancount.core.amount import Amount
from beancount.ingest import importer
from beancount.core.number import Decimal


class VisecaImporter(importer.ImporterProtocol):
    def __init__(self, account, currency="CHF", file_encoding="utf-8", manual_fixes=0):

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

        self.tags = {
            "Saldovortrag": {"EN": "Balance brought forward", "DE": "Saldovortrag"}
        }

    def name(self):
        return "Viseca {}".format(self.__class__.__name__)

    def file_account(self, file):
        return self.account

    def file_date(self, file):
        return datetime.now

    def identify(self, file):
        return "viseca" in file.name

    def extract(self, file, existing_entries=None):
        entries = []
        with open(file.name, encoding=self.file_encoding) as fd:

            reader = json.load(fd)
            for i, row in enumerate(reader["list"]):
                date = datetime.strptime(row["date"], "%Y-%m-%dT%H:%M:%S%z").date()
                amount = Amount(
                    Decimal(row["amount"]).quantize(Decimal("0.01")), row["currency"]
                )
                description = (
                    row["prettyName"] if "prettyName" in row else row["details"]
                )
                metadata = {"category": row["pfmCategory"]["name"]}

                if amount.number < 0:
                    # trans = Transaction(
                    #     data.new_metadata(file.name, i),
                    #     date,
                    #     self.FLAG,
                    #     "Viseca",
                    #     description,
                    #     data.EMPTY_SET,
                    #     data.EMPTY_SET,
                    #     [
                    #         Posting(
                    #             "Assets:CH:BCGE:Checking",
                    #             amount,
                    #             None,
                    #             None,
                    #             None,
                    #             None,
                    #         ),
                    #         Posting(self.account, -amount, None, None, None, None),
                    #     ],
                    # )
                    # entries.append(trans)
                    pass

                else:
                    trans = Transaction(
                        data.new_metadata(file.name, i, metadata),
                        date,
                        self.FLAG,
                        "",
                        description,
                        data.EMPTY_SET,
                        data.EMPTY_SET,
                        [
                            Posting(self.account, -amount, None, None, None, None),
                            Posting("Expenses:TBD", amount, None, None, None, None),
                        ],
                    )

                    if "serviceFees" in row:
                        serviceFees = row["serviceFees"]
                        assert all(
                            x["currency"] == serviceFees[0]["currency"]
                            for x in serviceFees
                        ), "All service fees must have the same currency"
                        fee_amount = Amount(
                            Decimal(
                                sum((Decimal(x["amount"])) for x in serviceFees)
                            ).quantize(Decimal("0.01")),
                            serviceFees[0]["currency"],
                        )
                        trans.postings.append(
                            Posting(
                                "Expenses:Financial:Fees",
                                fee_amount,
                                None,
                                None,
                                None,
                                None,
                            )
                        )

                    entries.append(trans)

        return entries
