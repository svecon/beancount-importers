import json
import re
from datetime import datetime

from beancount.core.data import Transaction
from beancount.core.data import Posting
from beancount.core import data
from beancount.core.amount import Amount
from beancount.ingest import importer
from beancount.core.number import Decimal


class BcgeImporter(importer.ImporterProtocol):
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
        return "Bcge {}".format(self.__class__.__name__)

    def file_account(self, file):
        return self.account

    def file_date(self, file):
        return datetime.now

    def identify(self, file):
        return "bcge" in file.name and file.name.endswith(".json")

    def extract(self, file, existing_entries=None):
        entries = []
        with open(file.name, encoding=self.file_encoding) as fd:

            reader = json.load(fd)
            for i, row in enumerate(reader["data"]):
                date = datetime.strptime(
                    row["bookingDate"], "%Y-%m-%dT%H:%M:%S.000Z"
                ).date()

                is_positive = Decimal(1.0) if row["type"] == "CREDIT" else Decimal(-1.0)
                amount = Amount(
                    is_positive
                    * Decimal(row["amount"]["value"]).quantize(Decimal("0.01")),
                    row["amount"]["currency"],
                )

                if "notification" in row and len(row["notification"]) > 1:
                    description = " ".join(row["notification"])
                else:
                    description = row["description"]

                description = " ".join(
                    x for x in description.split() if not re.match(r"^\d+$", x)
                )

                def extractPayee(row):
                    if "senderAddress" in row and row["senderAddress"]:
                        if row["senderAddress"][0] == "/C/0013347532":
                            return row["senderAddress"][1]
                        else:
                            return row["senderAddress"][0]
                    elif "beneficiaryAddress" in row and row["beneficiaryAddress"]:
                        return row["beneficiaryAddress"][0]
                    else:
                        return ""

                payee = extractPayee(row)
                metadata = {}

                if amount.number > 0:
                    if amount.number > 10000 and payee == "GOOGLE SWITZERLAND GMBH":
                        continue
                    trans = Transaction(
                        data.new_metadata(file.name, i, metadata),
                        date,
                        self.FLAG,
                        payee,
                        description,
                        data.EMPTY_SET,
                        data.EMPTY_SET,
                        [
                            Posting(
                                "Income:TBD",
                                -amount,
                                None,
                                None,
                                None,
                                None,
                            ),
                            Posting(self.account, amount, None, None, None, None),
                        ],
                    )
                    entries.append(trans)

                else:
                    trans = Transaction(
                        data.new_metadata(file.name, i, metadata),
                        date,
                        self.FLAG,
                        payee,
                        description,
                        data.EMPTY_SET,
                        data.EMPTY_SET,
                        [
                            Posting(self.account, amount, None, None, None, None),
                            Posting("Expenses:TBD", -amount, None, None, None, None),
                        ],
                    )

                    # if "serviceFees" in row:
                    #     serviceFees = row["serviceFees"]
                    #     assert all(
                    #         x["currency"] == serviceFees[0]["currency"]
                    #         for x in serviceFees
                    #     ), "All service fees must have the same currency"
                    #     fee_amount = Amount(
                    #         Decimal(
                    #             sum((Decimal(x["amount"])) for x in serviceFees)
                    #         ).quantize(Decimal("0.01")),
                    #         serviceFees[0]["currency"],
                    #     )
                    #     trans.postings.append(
                    #         Posting(
                    #             "Expenses:Financial:Fees",
                    #             fee_amount,
                    #             None,
                    #             None,
                    #             None,
                    #             None,
                    #         )
                    #     )
                    #
                    entries.append(trans)

        return entries
