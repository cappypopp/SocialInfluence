__author__ = 'cappy'

import xlsxwriter
import datetime
import json


class ExcelWriter(object):

    DEFAULT_DATE_FORMAT = "%m/%d/%Y %I:%M:%S %p"
    DATE_ONLY_DATE_FORMAT = "%m/%d/%Y"
    EXCEL_FILE_EXTENSION = ".xlsx"

    class ColumnFormat(object):
        """ Encapsulates the formatting and functions needed to format data for cells within an Excel column

        Attributes:
            workbook: XlsxWriter Workbook object
            name: column header name
            col_width: width of column in Excel units
            format_obj: properties object for use in Format() constructor
            data_format: lambda function to format data for use in column
            text_wrap: True: wrap text, False: don't
            excel_format_func: text name of XlsxWriter data writing method to add values to a cell
            excel_format_func_args: extra arguments for writing function, if any (like link name for write_url)

        """
        def __init__(self,
                     workbook=None,
                     name="",
                     col_width=11.3,
                     format_obj=None,
                     data_format=lambda x: x,
                     text_wrap=False,
                     excel_format_func="write_string",
                     excel_format_func_args=None):
            self.name = name
            self.col_width = col_width
            self.data_format = data_format
            self.text_wrap = text_wrap
            self.excel_format_func = excel_format_func
            self.excel_format_func_args = excel_format_func_args

            # set the excel workbook global format - nasty bug
            # here - original code passed raw objects to write_* methods
            # and there was no exception, unsure why. Need to call add_format
            # to create REAL 'Format' objects instead of raw property dicts

            self.format_obj = workbook.add_format(format_obj) if format_obj else None

    def __init__(self):
        self._ws = {}
        self._wb = None
        self._col_headers = []

    @staticmethod
    def excel_date(date_string):
        """ Returns an Excel date which is number of minutes since 1900 + some portion of a day
        :param date_string: string representation of a date - human-readable
        :return: float used in excel (like 45890.3)
        """
        temp = datetime.datetime(1899, 12, 30)
        date1 = datetime.datetime.strptime(date_string, ExcelWriter.DEFAULT_DATE_FORMAT)
        delta = date1 - temp
        return float(delta.days) + (float(delta.seconds) / 86400)

    def create_workbook(self, name):

        if ExcelWriter.EXCEL_FILE_EXTENSION not in name:
            name += ExcelWriter.EXCEL_FILE_EXTENSION

        self._wb = xlsxwriter.Workbook(name)

        post_id = ExcelWriter.ColumnFormat(workbook=self._wb,
                                           name="PostID",
                                           col_width=11.3,
                                           format_obj={"font_size": 9},
                                           data_format=lambda x: int(x),
                                           excel_format_func="write_number")

        post = ExcelWriter.ColumnFormat(workbook=self._wb,
                                        name="Post",
                                        col_width=5.5,
                                        format_obj={"font_size": 9,
                                                    "underline": 1,
                                                    "color": "blue"},
                                        excel_format_func="write_url",
                                        excel_format_func_args="LINK")

        post_date = ExcelWriter.ColumnFormat(workbook=self._wb,
                                             name="PostDate",
                                             col_width=13.5,
                                             format_obj={"font_size": 9,
                                                         "num_format": "mm/dd/yy hh:mm:ss"},
                                             data_format=lambda x:
                                             datetime.datetime.strptime(x, self.DEFAULT_DATE_FORMAT),
                                             excel_format_func="write_datetime")

        post_message = ExcelWriter.ColumnFormat(workbook=self._wb,
                                                name="PostMessage",
                                                col_width=50,
                                                text_wrap=True)

        post_date_day = ExcelWriter.ColumnFormat(workbook=self._wb,
                                                 name="DayOnlyDate",
                                                 col_width=13.5,
                                                 format_obj={"font_size": 9,
                                                             "num_format": "mm/dd/yy"},
                                                 data_format=lambda x:
                                                 datetime.datetime.strptime(x, self.DEFAULT_DATE_FORMAT),
                                                 excel_format_func="write_datetime")

        # shallow class attribute copies
        reply_date = ExcelWriter.ColumnFormat()
        reply_date.__dict__.update(post_date.__dict__)
        reply_date.name = "ReplyDate"

        reply_message = ExcelWriter.ColumnFormat()
        reply_message.__dict__.update(post_message.__dict__)
        reply_message.name = "ReplyMessage"

        post_from_zach = ExcelWriter.ColumnFormat(workbook=self._wb,
                                                  name="Zach",
                                                  col_width=8)

        post_from_aiyman = ExcelWriter.ColumnFormat()
        post_from_aiyman.__dict__.update(post_from_zach.__dict__)
        post_from_aiyman.name = "Aiyman"

        post_from_cz = ExcelWriter.ColumnFormat()
        post_from_cz.__dict__.update(post_from_zach.__dict__)
        post_from_cz.name = "CZ"

        post_was_escalation = ExcelWriter.ColumnFormat()
        post_was_escalation.__dict__.update(post_from_zach.__dict__)
        post_was_escalation.name = "Esc"


        gos = ExcelWriter.ColumnFormat(workbook=self._wb,
                                       name="GOS",
                                       col_width=4,
                                       data_format=lambda x: float(x),
                                       excel_format_func="write_number")

        # TODO: BRITTLE - the first 4 columns here must be in this order or the data from unanswered tweets will cause
        # an error when written (because data format won't match)
        self._col_headers = [post_id,
                             post,
                             post_date,
                             post_message,
                             post_date_day,
                             reply_date,
                             reply_message,
                             gos,
                             post_from_zach,
                             post_from_aiyman,
                             post_was_escalation,
                             post_from_cz]

    def add_sheet(self, name, sheet_data):

        worksheet = self._wb.add_worksheet(name)
        self._ws[name] = worksheet

        # only enumerate the headers for which we have columns of sheet_data
        # TODO: FIX to handle unanswered tweets! Only need 4 columns
        for i, header in enumerate(self._col_headers[0: len(sheet_data[0])]):

            fmt = self._wb.add_format()

            # text wrap and date formats don't mess up headers, font size does so it needs to go on cells
            # also, cell-based formatting overwrites column in most cases!
            if header.text_wrap:
                fmt.set_text_wrap()

            # set column width
            worksheet.set_column(i, i, header.col_width, fmt)

        self._add_table_to_worksheet(sheet_data, name)

        return worksheet

    def close_workbook(self):
        """
        MUST BE CALLED! Behavior indeterminate if it's not.
        :return: None
        """
        self._wb.close()

    def _add_data_to_table(self, worksheet, col_count, sheet_data):

        # write the data to each row
        """

        :param worksheet: Worksheet object
        :param col_count: number of columns in data set
        :param sheet_data: raw data to write to Worksheet
        """
        for row_num, row in enumerate(sheet_data):
            # we need to start the row number at 1 for Excel, there is no row 0
            rn = row_num + 1

            # for each row, write out the data in each cell
            for col_num in xrange(col_count):  # iterate over columns
                header = self._col_headers[col_num]  # get the header for this column

                # dynamic method call: we store the name of the formatting method we need from the Worksheet
                # class in the header objects. This allows us to get the instance of those methods by name so we
                # can call them dynamically.
                excel_cell_write_fn = getattr(worksheet, header.excel_format_func)

                # build function arguments - doing it this way so we can conditionally add the final argument if present
                arg_tuple = (rn,
                             col_num,
                             header.data_format(row[col_num]),  # call the lambda to format this cell
                             header.format_obj)  # the format dict for the Excel library (already a Format object)

                if header.excel_format_func_args:
                    # any add'l args to pass to the write fn will be appended in a singleton tuple (note the
                    # trailing comma - that's critical.) Since tuples are immutable you must assign back to the orig
                    # one to update it
                    arg_tuple = arg_tuple + (header.excel_format_func_args,)

                # writes the data to the cell
                excel_cell_write_fn(*arg_tuple)  # unpack the tuple into arguments using nifty syntax

    def _add_table_to_worksheet(self, sheet_data, name):
        """
        adds an Excel table to a worksheet
        :param sheet_data: raw data to add - just used to calculate dimensions
        :param name: name of worksheet on which to put table
        :return: None
        """

        col_count = len(sheet_data[0])
        table_options = self._build_table_options(col_count, name)  # build options object to pass to API
        row_count = len(sheet_data) + 1  # have to add one for the header row
        end_col_letter = chr(col_count + ord('A') - 1)  # get the final column letter (easier for debugging)
        table_range = "A1:{}{:d}".format(end_col_letter, row_count)  # build the table range in Excel letter notation
        worksheet = self[name]
        worksheet.add_table(table_range, table_options)  # add the table to the worksheet

        self._add_data_to_table(worksheet, col_count, sheet_data)

    def _build_table_options(self, length, name):

        # create a dict in the format required by Excel lib. We only want headers for the columns of
        # data we have, thus the slice
        """
        Builds options object for creating a table using the Excel library
        :rtype : table options object (dict)
        :param length: length of headers required (6 for first/support touch ,4 for unanswered)
        :param name: name of sheet; used to name table
        :return: options object
        """

        headers = [{'header': header.name} for header in self._col_headers[0:length]]

        table_options = {
            'style': 'Table Style Light 9',  # med blue background headers with white bold text, no banding, row lines
            'columns': headers,
            'name': "tbl" + name.translate(None, '- ')
        }
        return table_options

    def __getitem__(self, item):
        """
        indexer for this class
        :param item: name of worksheet
        :return: worksheet object specified by name
        """
        if not isinstance(item, basestring):
            raise ValueError("sheet name required")
        else:
            return self._ws[item]

"""
if __name__ == '__main__':
    twitter_data_dir = "twitter-gos-data"
    tweets_file = "{}/TW-First Touch Data.json".format(twitter_data_dir)

    if True:

        with open(tweets_file, "rb") as fp:
            data = json.load(fp)
        ew = ExcelWriter()

        ew.create_workbook("{}/{}".format(twitter_data_dir, "ExcelWriterTest.xlsx"))
        ew.add_sheet("dummy", data)
        ew.close_workbook()
    else:
        wb = xlsxwriter.Workbook("{}/{}".format(twitter_data_dir, "ExcelWriterTest.xlsx"))
        ws = wb.add_worksheet("dummy")
        header_row = [{'header': hdr} for hdr in ["one", "two", "three"]]

        options = {
            'style': 'Table Style Light 9',  # med blue background headers with white bold text, no banding, row lines
            'columns': header_row,
            'name': "tbl"
        }
        tbl_range = "A1:C2"

        ws.add_table(tbl_range, options)

        ws.write_number(1, 0, int("3"), wb.add_format({"font_size": 9}))

        wb.close()
"""
