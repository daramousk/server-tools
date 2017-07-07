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
        ' (select partner_id from res_company)',
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
    def unlink(self):
        """Delete a company, with all its data, from the database.

        Module uses SQL for performance reasons. Only company itself
        is unlinked using the orm.
        """
        cr = self.env.cr
        # Check wether at least one company remains
        remaining_company = self.search([('id', 'not in', self.ids)])
        if not remaining_company:
            raise ValidationError(_(
                "At least one company should remain!"
            ))
        remaining_company_id = remaining_company[0].id
        # Change users now working in companies to be deleted:
        _logger.debug(
            _("Users will be transferred to company %d"), remaining_company_id
        )
        user_model = self.env['res.users']
        users_to_change = user_model.search([
            ('company_id', 'in', self.ids)
        ])
        if users_to_change:
            for user in users_to_change:
                if remaining_company_id not in user.company_ids.ids:
                    user.write({'company_ids': [(4, remaining_company_id)]})
            users_to_change.write({'company_id': remaining_company_id})
        # No partner if it is a user should belong to company to be deleted:
        users_to_change = user_model.search([])
        for user in users_to_change:
            if user.partner_id.company_id and \
                    user.partner_id.company_id.id in self.ids:
                user.partner_id.write({
                    'company_id': remaining_company_id,
                })
        # Update  
        do_prepurge(cr)
        for this in self:
            for tablename in COMPANY_TABLES:
                delete_company_rows(cr, tablename, this.id)
        super(ResCompany, self).unlink()
