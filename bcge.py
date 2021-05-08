import datetime
import decimal
import os
import re


from collections import OrderedDict
from lxml import etree
from beancount.core import amount, data
from beancount.core.data import Transaction, Entries, Directive, Posting, Meta, Balance
from beancount.core.number import D
from beancount_import.amount_parsing import parse_amount
from beancount_import.matching import FIXME_ACCOUNT
from beancount_import.source import ImportResult, Source, InvalidSourceReference, SourceResults, AssociatedData, LogFunction, description_based_source
from beancount_import.journal_editor import JournalEditor


class BCGECAMTSource(description_based_source.DescriptionBasedSource):
    def __init__(self, directory: str,
                 account: str,
                 salary_account: str,
                 fees_account: str,
                 atm_account: str,
                 **kwargs):
        super(BCGECAMTSource, self).__init__(**kwargs)
        self.account = account
        self.salary_account = salary_account
        self.fees_account = fees_account
        self.atm_account = atm_account
        self.directory = directory
        self.xpathkwargs = {'namespaces': {
            'camt': 'urn:iso:std:iso:20022:tech:xsd:camt.053.001.04'}}

    @property
    def name(self) -> str:
        return "BCGE CAMT Importer"

    def prepare(self, journal: JournalEditor, results: SourceResults) -> None:
        results.add_account(self.account)
        seen_txns = set()
        skipped_entries = 0
        for entry in journal.all_entries:
            if not isinstance(entry, Transaction):
                skipped_entries += 1
                continue
            seen_txns.add(entry.meta.get('txn_id'))
        print("{} entries in seen set, {} skipped".format(
            len(seen_txns), skipped_entries))

        new_entries = []
        for filename in os.listdir(self.directory):
            new_entries.extend(self.handle_file(
                os.path.join(self.directory, filename)))

        for entry in new_entries:
            if not isinstance(entry, Transaction):
                continue
            if not entry.meta.get('txn_id') in seen_txns:
                results.add_pending_entry(ImportResult(date=entry.date, entries=[entry], info={
                                          'type': 'application/camt', 'filename': 'whokows'}))

    # def is_posting_cleared(self, posting, *args, **kwargs):
    #    return posting.

    def handle_file(self, filename):
        self.filename = filename
        self.entry_counter = 0
        doc = etree.parse(filename)
        entries = []
        stmts = doc.xpath(
            '/camt:Document/camt:BkToCstmrStmt/camt:Stmt', **self.xpathkwargs)
        for stmt in stmts:
            entries.extend(self._handle_stmt(stmt))
        return entries

    def get_meta(self, txn_id=None):
        meta = OrderedDict()
        meta['filename'] = self.filename
        meta['lineno'] = self.entry_counter
        self.entry_counter += 1

        if txn_id:
            meta['txn_id'] = txn_id
        return meta

    def _handle_balance(self, account, bal):
        meta = self.get_meta()
        amt = bal.xpath('./camt:Amt', **self.xpathkwargs)[0]
        units = amount.Amount(decimal.Decimal(amt.text), amt.attrib['Ccy'])
        date_string = bal.xpath('./camt:Dt/camt:Dt', **
                                self.xpathkwargs)[0].text
        date = datetime.datetime.strptime(date_string, '%Y-%m-%d').date()
        return Balance(meta, date, account, units, None, None)

    def _handle_tx(self, account, date, tx, entry_txn_id=None):
        txn_id = None
        if entry_txn_id:
            txn_id = entry_txn_id
        else:
            txn_ids = tx.xpath('.//camt:AcctSvcrRef/text()',
                               **self.xpathkwargs)
            if not txn_ids:
                subtypes = tx.xpath(
                    './camt:BkTxCd/camt:Domn/camt:Fmly/camt:SubFmlyCd/text()', **self.xpathkwargs)
                if subtypes and subtypes[0] == 'SALA':
                    txn_ids = ['SALA-{}'.format(date.isoformat())]
                else:
                    raise ValueError(
                        'Transaction has no ref: {}'.format(etree.tostring(tx)))
            txn_id = txn_ids[0]
        meta = self.get_meta(txn_id)
        amt = tx.xpath('./camt:Amt', **self.xpathkwargs)[0]
        units = amount.Amount(decimal.Decimal(amt.text), amt.attrib['Ccy'])
        credit = tx.xpath('./camt:CdtDbtInd', **
                          self.xpathkwargs)[0].text == 'CRDT'
        if not credit:
            units = -units

        payee = None
        text = ''
        dest_account = FIXME_ACCOUNT

        counterparty_names = tx.xpath('./camt:RltdPties/camt:{}/camt:Nm/text()'.format(
            'Dbtr' if credit else 'Cdtr'), **self.xpathkwargs)
        subtypes = tx.xpath(
            './camt:BkTxCd/camt:Domn/camt:Fmly/camt:SubFmlyCd/text()', **self.xpathkwargs)

        txinf = tx.xpath('./camt:AddtlTxInf/text()', **self.xpathkwargs)
        ntryinf = tx.xpath('./camt:AddtlTxInf/text()', **self.xpathkwargs)
        inf = txinf or ntryinf

        instrid = tx.xpath(
            './camt:Refs/camt:InstrId/text()', **self.xpathkwargs)

        if subtypes and subtypes[0] == 'SALA':
            dest_account = self.salary_account
            text = 'Salary'
        elif inf and inf[0].startswith('Maestro purchase'):
            text = inf[0]
            maestro_match = re.match(
                r'Maestro purchase [0-9]{2}.[0-9]{2}.[0-9]{4} [0-9]{2}:[0-9]{2} (.*) Card number: .*', inf[0])
            if maestro_match:
                payee = maestro_match.group(1)
        elif inf and inf[0].startswith('Cash Point'):
            text = 'ATM Withdrawal: {}'.format(inf[0])
            cash_match = re.match(
                r'Cash Point [0-9]{2}.[0-9]{2}.[0-9]{4} [0-9]{2}:[0-9]{2} (.*) Card number: .*', inf[0])
            if cash_match:
                text = 'ATM Withdrawal: {}'.format(cash_match.group(1))
            dest_account = self.atm_account
        elif inf and inf[0].startswith('Twint '):
            text = inf[0]
            twint_match = re.match(r'Twint (.*) [0-9]+', inf[0])
            if twint_match:
                payee = twint_match.group(1)
            else:
                payee = inf[0][6:]
        elif counterparty_names and counterparty_names[0] != 'NOTPROVIDED':
            payee = counterparty_names[0]

        if (not text) and instrid and instrid[0] not in {'00000000', 'NOTPROVIDED'}:
            text = instrid[0]

        extra_meta = {}
        if payee:
            extra_meta['source_desc'] = payee
        elif text:
            extra_meta['source_desc'] = text

        return Transaction(meta, date, '*', payee, text, data.EMPTY_SET, set(), [
            Posting(account, units, None, None, None, meta=extra_meta),
            Posting(dest_account, -units, None, None, None, None),
        ])

    def _handle_ntry(self, account, ntry):
        date_string = ntry.xpath(
            './camt:BookgDt/camt:Dt/text()', **self.xpathkwargs)[0]
        date = datetime.datetime.strptime(date_string, '%Y-%m-%d').date()
        tx_dtls = ntry.xpath('./camt:NtryDtls/camt:TxDtls', **self.xpathkwargs)
        refs = ntry.xpath('./camt:AcctSvcrRef/text()', **self.xpathkwargs)
        kwargs = {}
        if refs and len(tx_dtls) == 1:
            kwargs['entry_txn_id'] = refs[0]

        if tx_dtls:
            return [self._handle_tx(account, date, tx, **kwargs) for tx in tx_dtls]
        return [self._handle_tx(account, date, ntry, **kwargs)]

    def _handle_stmt(self, stmt):
        entries = []

        ntrys = stmt.xpath('./camt:Ntry', **self.xpathkwargs)
        for ntry in ntrys:
            entries.extend(self._handle_ntry(self.account, ntry))

        balances = stmt.xpath('./camt:Bal', **self.xpathkwargs)
        entries.extend(self._handle_balance(self.account, bal)
                       for bal in balances)

        return entries


def load(spec, log_status):
    return BCGECAMTSource(log_status=log_status, **spec)
