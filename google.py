import datetime
import glob
import locale
import re
from typing import List

from absl import app
from absl import flags
from beancount.core import data
from beancount.core.amount import Amount
from beancount.core.number import Decimal
from beancount.parser import printer
from pdfreader import SimplePDFViewer
from pdfreader.viewer import PageDoesNotExist

BRUTTO_INCOME_ACCOUNT = "Income:Salary:Google:CH:Brutto"
NETTO_INCOME_ACCOUNT = "Income:Salary:Google:CH:Netto"
SALARY_TYPES = {
    "Monthly Salary",
    "13th Month Payout",
    "Transportation Allowance",
    "Health Insurance Contribution",
    "Meal Allowance (Net)",
    "Relocation BIK (Net)",
    "Friends & Family (Gross Up)",
    "Friends & Family (Net)",
    "Massage (Net)",
    "Company Bonus Plan",
    "Patent Award Gross",
    "Spot Bonus Gross",
    "Peer Bonus",
}
COLUNM_TO_ACCOUNT = {
    "Swiss Social Security (AHV/IV/EO)": "Expenses:Taxes:CH:AHV",
    "Unemployment Insurance": "Expenses:Insurance:Unemployment",
    "Unemployment Insurance compl.": "Expenses:Insurance:Unemployment",
    "Pension Fund": "Assets:CH:AXA",
    "Tax at Source": "Expenses:Taxes:CH:TaxAtSource",
    "MSSB/CS Withholding": "Expenses:Taxes:CH",
    "additional run adjustment": "Expenses:Taxes:CH",
    "Employee Overpayment Recoup": BRUTTO_INCOME_ACCOUNT,
    "Deduction Net Amount": BRUTTO_INCOME_ACCOUNT,
    "Gcard Repayment": BRUTTO_INCOME_ACCOUNT,
    "G Give charitable donation": "Expenses:Donation",
    "TOTAL": NETTO_INCOME_ACCOUNT,
}
FLAGS = flags.FLAGS
flags.DEFINE_string("output", "ledger/google.bean",
                    "Output bean file. Will be overwritten.")


def pares_amount(s: str) -> Amount:
    s = re.sub(r"[\s']", "", s)
    return Amount(locale.atof(s, Decimal), "CHF")


def get_amount_for(strings: List[str], idx: int) -> Amount:
    try:
        if len(strings[idx + 1]) - strings[idx + 1].index(".") == 5:
            # idx+1 contains the rate with 4 decimals points. The next field has the
            # absolute number.
            return pares_amount(strings[idx + 2])
        return pares_amount(strings[idx + 1])
    except Exception:
        print(strings[idx - 1:idx + 3])
    raise

def make_posting(account: str, amount: Amount, meta=None) -> data.Posting:
    return data.Posting(
        account=account,
        units=amount,
        cost=None,
        price=None,
        flag=None,
        meta=meta)


def page_to_transaction(fn: str, v: SimplePDFViewer) -> data.Transaction:
    print(f"Parsing file {fn} page {v.current_page_number}.")
    v.render()
    date = None
    period = None
    postings = []
    for i, text in enumerate(v.canvas.strings):
    if text == "Period":
        period = v.canvas.strings[i + 1]
        continue
    if text == "Date of payment":
        date = datetime.datetime.strptime(v.canvas.strings[i + 2],
                                          "%d.%m.%Y").date()
        continue
    meta = data.new_metadata(fn, i, {"salary_element": text})
    if text in SALARY_TYPES:
        amount = get_amount_for(v.canvas.strings, i)
        postings += [make_posting(BRUTTO_INCOME_ACCOUNT, -amount, meta)]
        continue
    if text == "Tax/soc. sec. paid by employer":
        amount = get_amount_for(v.canvas.strings, i - 2)
        postings += [make_posting(BRUTTO_INCOME_ACCOUNT, -amount, meta)]
        continue
    if text.startswith("Overtime/Special Pay - "):
        amount = pares_amount(v.canvas.strings[i + 2])
        postings += [make_posting(BRUTTO_INCOME_ACCOUNT, -amount, meta)]
        continue
    if text in COLUNM_TO_ACCOUNT:
        amount = get_amount_for(v.canvas.strings, i)
        postings += [make_posting(COLUNM_TO_ACCOUNT[text], amount, meta)]
        continue
    assert date, date
    assert period, period
    assert postings, v.canvas.strings
    return data.Transaction(
        meta=data.new_metadata(fn, v.current_page_number),
        date=date,
        flag="*",
        payee="",
        narration=f"Payslip for period {period}",
        tags=data.EMPTY_SET,
        links=data.EMPTY_SET,
        postings=postings)


def main(argv):
    del argv
    files = glob.glob("payslips/Google/20*/*.pdf")
    entries = []
    for fn in sorted(files):
    v = SimplePDFViewer(open(fn, "rb"))
    while True:
        entries.append(page_to_transaction(fn, v))
        try:
        v.next()    # pylint: disable=not-callable
        except PageDoesNotExist:
        break

    entries = sorted(entries, key=data.entry_sortkey)
    if len(entries) < 10:
    printer.print_entries(entries)
    with open(FLAGS.output, "w") as f:
    f.write('plugin "beancount.plugins.auto_accounts"\n')
    printer.print_entries(entries, file=f)


if __name__ == "__main__":
    app.run(main)
