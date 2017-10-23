# -*E coding: utf-8 -*-
# Copyright 2017 Therp BV <https://therp.nl>.
# License AGPL-3.0 or later (http://www.gnu.org/licenses/agpl.html).
import logging

from openerp import api, models, _
from openerp.exceptions import ValidationError


_logger = logging.getLogger(__name__)


# Commands to deal with inconsistencies in database:
PRE_PURGE_COMMANDS = [
    'UPDATE res_partner rp SET company_id = NULL'
    ' WHERE id IN (SELECT partner_id FROM account_invoice ai'
    '  JOIN res_partner rp2 ON ai.partner_id = rp2.id'
    '  WHERE NOT ai.company_id IS NULL and NOT rp2.company_id IS NULL'
    '    AND ai.company_id <> rp2.company_id)',
    'UPDATE res_partner rp SET company_id = NULL'
    ' WHERE id IN (SELECT partner_id FROM purchase_order po'
    '  JOIN res_partner rp2 ON po.partner_id = rp2.id'
    '  WHERE NOT po.company_id IS NULL and NOT rp2.company_id IS NULL'
    '    AND po.company_id <> rp2.company_id)',
]


# Put tables in desired order of deletion:
COMPANY_TABLES = [
    'sale_order',
    'purchase_order',
    'procurement_order',
    'stock_picking',
    'stock_picking_type',
    'stock_warehouse',
    'stock_quant',
    'stock_location',
    'account_analytic_line',
    'account_invoice',
    'account_move',
    'account_voucher',
    'account_voucher_line',
    'account_analytic_account',
    'account_analytic_journal',
    'account_fiscal_position',
    'account_account',
    'account_bank_statement_line',
    'account_bank_statement',
    'account_journal',
    'account_period',
    'account_fiscalyear',
    'account_tax_code',
    'account_tax',
    'product_price_history',
    'product_template',
    'res_partner',
    'res_currency',
    'ir_property',
    'ir_attachment',
    'ir_default',
    'ir_values',
    'multi_company_default',
]

PRE_TABLE_COMMANDS = {
    'account_voucher': [
        'delete from account_voucher where account_id in '
        ' (select id from account_account where company_id = %s)',
        'delete from account_voucher where journal_id in '
        ' (select id from account_journal where company_id = %s)',
    ],
    'account_voucher_line': [
        'delete from account_voucher_line where account_id in '
        ' (select id from account_account where company_id = %s)',
    ],
}

DELETE_COMMANDS = {
    'purchase_order':
        'delete from purchase_order where picking_type_id in'
        ' (select id from stock_picking_type where warehouse_id in'
        ' (select id from stock_warehouse where company_id = %s))',
    'stock_picking':
        'delete from stock_picking where picking_type_id in'
        ' (select id from stock_picking_type where warehouse_id in'
        ' (select id from stock_warehouse where company_id = %s))',
    'stock_picking_type':
        'delete from stock_picking_type where warehouse_id in'
        ' (select id from stock_warehouse where company_id = %s)',
    'account_bank_statement_line':
        'delete from account_bank_statement_line where statement_id in'
        ' (select id from account_bank_statement where company_id = %s)',
    'res_partner':
        'delete from res_partner where company_id = %s and id not in'
        ' (select partner_id from res_company) and'
        ' id not in (select partner_id from res_users)',
}


def table_exists(cr, tablename):
    """Check whether a certain table or view exists."""
    cr.execute('SELECT 1 FROM pg_class WHERE relname = %s', (tablename,))
    return cr.fetchone()


def do_prepurge(cr):
    """Prepurge commands."""
    for statement in PRE_PURGE_COMMANDS:
        _logger.debug(_("Executing prepurge command %s"), statement)
        cr.execute(statement)


def delete_company_rows(
        cr, tablename, company_id, fieldname='company_id'):
    """Delete all rows for a company from the table.

    For the moment assume that all references to company are through a
    field called company_id.
    """
    if not table_exists(cr, tablename):
        return
    _logger.debug(_("Deleting rows from table %s"), tablename)
    for statement in PRE_TABLE_COMMANDS.get(tablename, []):
        cr.execute(statement, (company_id,))
    statement = DELETE_COMMANDS.get(
        tablename,
        "DELETE FROM %s WHERE %s =" % (tablename, fieldname) + ' %s'
    )
    cr.execute(statement, (company_id,))


class ResCompany(models.Model):
    _inherit = 'res.company'

    @api.multi
    def move_users(self, remaining_company):
        cr = self.env.cr
        # Change users now working in companies to be deleted:
        _logger.debug(
            _("Users will be transferred to company %d"),
            remaining_company.id
        )

        # select the users that belong in in the current company
        # and they are connected with the remaining_company
        # move them to the remaining_company.
        # and change their partner_id.company_id to remaining_company
        statement_select_users = """
        SELECT id FROM res_users WHERE company_id=ANY(%s)
        """
        cr.execute(statement_select_users, (self.ids, ))   
        select_results = cr.fetchall()
        statement_update_users = """
        UPDATE res_users SET company_id = %s
        WHERE res_users.id=ANY(%s)
        """
        cr.execute(statement_update_users, (remaining_company.id,
                                            select_results))
        # insert a connection between the user and the remaining company
        statement_update_company_ids = """
            INSERT INTO res_company_users_rel VALUES (%s, %s)
            ON CONFLICT DO NOTHING
            """
        for row in select_results:
            cr.execute(statement_update_company_ids, (remaining_company.id,
                                                      row[0]))
        statement_update_partners = """
        UPDATE res_partner SET company_id = %s WHERE id = ANY(
        SELECT res_users.partner_id FROM res_partner
        INNER JOIN res_users ON res_users.partner_id = res_partner.id
        WHERE res_users.id = ANY(%s))
        """
        cr.execute(statement_update_partners, (remaining_company.id,
                                          select_results))

    @api.multi
    def unlink(self):
        """Delete a company, with all its data, from the database.

        Module uses SQL for performance reasons. Only company itself
        is unlinked using the orm.
        """
        cr = self.env.cr
        # Check wether at least one company remains
        remaining_companies = self.search([('id', 'not in', self.ids)])
        if not remaining_companies:
            raise ValidationError(_(
                "At least one company should remain!"
            ))
        self.move_users(remaining_companies[0])
        # Update
        do_prepurge(cr)
        for this in self:
            for tablename in COMPANY_TABLES:
                delete_company_rows(cr, tablename, this.id)
        super(ResCompany, self).unlink()
