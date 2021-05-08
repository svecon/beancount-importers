import csv
import datetime
import decimal
import json
import os
import re

from collections import OrderedDict

from lxml import etree
import hashlib

from beancount.core.data import Transaction, Entries, Directive, Posting, Meta, Balance
from beancount.core import amount, data
from beancount.core import position
from beancount.core.number import D
from beancount_import.posting_date import POSTING_DATE_KEY
from beancount_import.amount_parsing import parse_amount
from beancount_import.matching import FIXME_ACCOUNT
from beancount_import.source import ImportResult, Source, InvalidSourceReference, SourceResults, AssociatedData, LogFunction, description_based_source
from beancount_import.journal_editor import JournalEditor

def hexmd5(input):
    thing = hashlib.md5()
    thing.update(','.join(input).encode('utf-8'))
    return thing.hexdigest()

class IBSource(description_based_source.DescriptionBasedSource):
    def __init__(self, directory: str, account: str, **kwargs):
        super(IBSource, self).__init__(**kwargs)
        self.directory = directory
        self.account = account

    @property
    def name(self) -> str:
        return "IB CSV importer"
    
    def prepare(self, journal: JournalEditor, results: SourceResults):
        results.add_account(self.account)
        seen_txns = set()
        skipped_entries = 0
        for entry in journal.all_entries:
            if not isinstance(entry, Transaction) or not entry.meta.get('txn_id_ib'): 
                skipped_entries += 1
                continue
            seen_txns.add(entry.meta.get('txn_id_ib'))
        print("{} entries in seen set, {} skipped".format(len(seen_txns), skipped_entries))
        
        new_entries = []
        for filename in os.listdir(self.directory):
            new_entries.extend(self.handle_file(os.path.join(self.directory, filename)))
        
        for entry in new_entries:
            if not isinstance(entry, Transaction): continue
            if not entry.meta.get('txn_id_ib') in seen_txns:
                results.add_pending_entry(ImportResult(date=entry.date, entries=[entry], info={'type': 'application/csv', 'filename':'ibfile'}))
    
    def handle_file(self, file_path):
        with open(file_path, 'r') as infile:
            lines = list(csv.reader(infile))
        entries = []
        cur_section = None
        cur_header = None
        portfolio = {}
        for index, line in enumerate(lines):
            split_line = line
            if split_line[1] == 'Header':
                cur_section = split_line[0]
                cur_header = split_line[1:]
                continue
            line_dict = dict(zip(cur_header, split_line[1:]))

            meta = data.new_metadata(file_path, index)
            meta['txn_id_ib'] = hexmd5(line)
            if cur_section == 'Deposits & Withdrawals' and len(line_dict['Currency']) == 3:
                date = datetime.datetime.strptime(line_dict['Settle Date'], '%Y-%m-%d').date()
                desc = line_dict['Description']
                units = amount.Amount(D(line_dict['Amount']), line_dict['Currency'])
                entries.append(data.Transaction(
                    meta, date, '*', None, desc, data.EMPTY_SET, set(), [
                        data.Posting(self.account, units, None, None, None, None),
                        data.Posting('Expenses:FIXME', -units, None, None, None, None),
                    ]))
            if cur_section == 'Fees' and not line_dict['Subtitle'].startswith('Total') and not line_dict['Header'].startswith('Notes'):
                date = datetime.datetime.strptime(line_dict['Date'], '%Y-%m-%d').date()
                desc = line_dict['Description']
                units = amount.Amount(D(line_dict['Amount']), line_dict['Currency'])
                entries.append(data.Transaction(
                    meta, date, '*', None, desc, data.EMPTY_SET, set(), [
                        data.Posting(self.account, units, None, None, None, None),
                        data.Posting('Expenses:Fees:Brokerage', -units, None, None, None, None),
                    ]))
            if cur_section == 'Dividends' and 'total' not in line_dict['Currency'].lower():
                date = datetime.datetime.strptime(line_dict['Date'], '%Y-%m-%d').date()
                desc = line_dict['Description']
                units = amount.Amount(D(line_dict['Amount']), line_dict['Currency'])
                entries.append(data.Transaction(
                    meta, date, '*', None, desc, data.EMPTY_SET, set(), [
                        data.Posting(self.account, units, None, None, None, None),
                        data.Posting('Income:Dividends', -units, None, None, None, None),
                    ]))
            if cur_section == 'Withholding Tax' and 'total' not in line_dict['Currency'].lower():
                date = datetime.datetime.strptime(line_dict['Date'], '%Y-%m-%d').date()
                desc = line_dict['Description']
                units = amount.Amount(D(line_dict['Amount']), line_dict['Currency'])
                entries.append(data.Transaction(
                    meta, date, '*', None, desc, data.EMPTY_SET, set(), [
                        data.Posting(self.account, units, None, None, None, None),
                        data.Posting('Expenses:Tax:Withholding', -units, None, None, None, None),
                    ]))
            if cur_section == 'Interest' and 'total' not in line_dict['Currency'].lower():
                date = datetime.datetime.strptime(line_dict['Date'], '%Y-%m-%d').date()
                desc = line_dict['Description']
                units = amount.Amount(D(line_dict['Amount']), line_dict['Currency'])
                entries.append(data.Transaction(
                    meta, date, '*', None, desc, data.EMPTY_SET, set(), [
                        data.Posting(self.account, units, None, None, None, None),
                        data.Posting('Expenses:Fees:BrokerageInterest', -units, None, None, None, None),
                    ]))
            if cur_section == 'Trades' and line_dict['DataDiscriminator'] == 'Order' and line_dict['Asset Category'].startswith('Forex'):
                symbol = line_dict['Symbol']
                cur1, cur2 = line_dict['Symbol'].split('.')
                quantity = D(line_dict['Quantity'].replace(',', ''))
                trade_price = D(line_dict['T. Price'])
                fees = amount.Amount(abs(D(line_dict['Comm in EUR'])), 'EUR')
                increment_cur1 = amount.Amount(quantity, cur1)
                increment_cur2 = amount.Amount(D(line_dict['Proceeds']), cur2)
                date = datetime.datetime.strptime(line_dict['Date/Time'], '%Y-%m-%d, %H:%M:%S').date()
                desc = '{} - {} @ {}'.format(quantity, symbol, line_dict['T. Price'])

                postings = [
                    data.Posting(self.account, increment_cur1, None, amount.Amount(trade_price, cur2), None, None),
                    data.Posting(self.account, increment_cur2, None, None, None, None),
                    data.Posting('Expenses:Fees:Brokerage', fees, None, None, None, None),
                    data.Posting(self.account, -fees, None, None, None, None),
                ]
                entries.append(data.Transaction(
                    meta, date, '*', None, desc, data.EMPTY_SET, set(), postings))

            if cur_section == 'Trades' and line_dict['DataDiscriminator'] == 'Order' and (line_dict['Asset Category'].startswith('Stocks') or line_dict['Asset Category'].startswith('Equity and Index Options')):
                is_options = 'Options' in line_dict['Asset Category']
                symbol = line_dict['Symbol'].replace(' ', '_')
                quantity = D(line_dict['Quantity'].replace(',', ''))
                trade_price = D(line_dict['T. Price'])
                if is_options:
                    trade_price *= 100
                currency = line_dict['Currency']
                rate = position.Cost(trade_price, currency, None, None)
                fees = amount.Amount(abs(D(line_dict['Comm/Fee'])), line_dict['Currency'])
                date = datetime.datetime.strptime(line_dict['Date/Time'], '%Y-%m-%d, %H:%M:%S').date()
                desc = '{} - {} @ {}'.format(quantity, symbol, line_dict['T. Price'])
                cash_amount = amount.Amount(D(line_dict['Proceeds']) + D(line_dict['Comm/Fee']), currency)
                if quantity > 0:
                    basis = None
                else:
                    basis = rate
                if symbol not in portfolio:
                    portfolio[symbol] = 0
                postings = [
                    data.Posting(self.account, cash_amount, None, None, None, None),
                    data.Posting('Expenses:Fees:Brokerage', fees, None, None, None, None),
                    data.Posting('Assets:Stock', amount.Amount(D(quantity), symbol), basis, amount.Amount(trade_price, currency), None, None),
                ]
                if trade_price == 0 or (abs(portfolio[symbol]) + quantity < abs(portfolio[symbol])):
                    postings.append(data.Posting('Income:TradingProfit', None, None, None, None, None))
                entries.append(data.Transaction(
                    meta, date, '*', None, desc, data.EMPTY_SET, set(), postings))
                portfolio[symbol] += quantity

        return entries

def load(spec, log_status):
    return IBSource(log_status=log_status, **spec)