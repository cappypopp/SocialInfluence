#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'cappy'

import unittest
import logging
import json
import os
import os.path
from datetime import datetime
from mock import patch
from tlinsights.constants import APP
from tlinsights import utils
import excelwriter

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)  # should only be called once in entire app


class TestExcelWriter(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        with open("../" + APP.FIRST_TOUCH_FILE, "r") as fp:
            data = json.load(fp, encoding="utf-8")

            cls.sheet_data = data[:20] if len(data) > 20 else data[:] # just peel off 20 items at most

        cls.TEST_DIR = u"test-data"
        cls.TEST_WORKBOOK = u"test-workbook.xlsx"
        cls.TEST_WORKBOOK_NO_EXT = u"test-workbook"
        cls.TEST_WORKBOOK_PATH = u"{}/{}".format(cls.TEST_DIR, cls.TEST_WORKBOOK)
        cls.TEST_WORKBOOK_PATH_NO_EXT = u"{}/{}".format(cls.TEST_DIR, cls.TEST_WORKBOOK_NO_EXT)

        cls.XLSXWRITER_WORKBOOK_MOCK_PATH = "excelwriter.xlsxwriter.Workbook"

        if not os.path.exists(cls.TEST_DIR):
            os.mkdir(cls.TEST_DIR)

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.TEST_DIR):
            os.removedirs(cls.TEST_DIR)

    @utils.logged()
    def test_create_instance(self):
        inst = excelwriter.ExcelWriter()
        self.assertIsNotNone(inst)

    @utils.logged()
    def test_created_instance_fields(self):
        inst = excelwriter.ExcelWriter()
        self.assertEqual(len(inst._col_headers), 0)
        self.assertIsNone(inst._wb)
        self.assertIsNotNone(inst._ws)
        self.assertEqual(len(inst._ws), 0)

    @utils.logged()
    def test_data_created(self):
        self.assertIsNotNone(self.sheet_data)
        logger.debug(self.sheet_data[0])

    @utils.logged()
    def test_data_row_expected_length(self):
        self.assertEqual(len(self.sheet_data[0]), 12)

    @utils.logged()
    def test_data_expected_length(self):
        self.assertLess(len(self.sheet_data), 21)

    @utils.logged()
    def test_create_test_workbook_INTEGRATION(self):
        """
        INTEGRATION TEST - touches file system and uses xlsxwriter class indirectly
        :return:
        """
        inst = excelwriter.ExcelWriter()
        inst.create_workbook(self.TEST_WORKBOOK_PATH)
        self.assertIsNotNone(inst._wb)
        inst.close_workbook()
        self.assertTrue(os.path.isfile(self.TEST_WORKBOOK_PATH))
        os.remove(self.TEST_WORKBOOK_PATH)
        self.assertFalse(os.path.isfile(self.TEST_WORKBOOK_PATH))

    @utils.logged()
    def test_sheet_created(self):
        sheet_name = 'dummy'
        with patch(self.XLSXWRITER_WORKBOOK_MOCK_PATH) as mock_wb:
            inst = excelwriter.ExcelWriter()
            inst.create_workbook(self.TEST_WORKBOOK_PATH)
            ws = inst.add_sheet(sheet_name, self.sheet_data)
            self.assertIsNotNone(ws)
            inst.close_workbook()

    @utils.logged()
    def test_sheet_indexer(self):
        sheet_name = 'dummy'
        with patch(self.XLSXWRITER_WORKBOOK_MOCK_PATH) as mock_wb:
            inst = excelwriter.ExcelWriter()
            inst.create_workbook(self.TEST_WORKBOOK_PATH)
            ws = inst.add_sheet(sheet_name, self.sheet_data)
            self.assertIsNotNone(ws[sheet_name])
            inst.close_workbook()

    @utils.logged()
    def test_excel_date(self):
        dt = datetime.now()
        dts = dt.strftime(excelwriter.ExcelWriter.DEFAULT_DATE_FORMAT)
        et = excelwriter.ExcelWriter.excel_date(dts)
        self.assertIsNotNone(et)
        self.assertGreater(et, 42026)  # 42026 was date returned first time I ran this test
        self.assertIsInstance(et, float)

    @utils.logged()
    def test_create_sheet_no_extension(self):
        with patch(self.XLSXWRITER_WORKBOOK_MOCK_PATH) as mock_wb:
            inst = excelwriter.ExcelWriter()
            inst.create_workbook(self.TEST_WORKBOOK_PATH_NO_EXT)
            self.assertIsNotNone(inst._wb)

    @utils.logged()
    def test_indexer_no_name(self):
        with patch(self.XLSXWRITER_WORKBOOK_MOCK_PATH) as mock_wb:
            inst = excelwriter.ExcelWriter()
            inst.create_workbook(self.TEST_WORKBOOK_PATH)
            with self.assertRaises(KeyError):
                ws = inst['']
            with self.assertRaises(ValueError):
                ws = inst[123]