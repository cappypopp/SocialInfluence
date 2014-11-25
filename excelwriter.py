__author__ = 'cappy'

import xlsxwriter
import datetime
import json
from xlsxwriter.format import Format

class ExcelWriter:

    DEFAULT_DATE_FORMAT = "%m/%d/%Y %I:%M:%S %p"

    # could make this a class, but probably more verbose...

    class ColumnFormat:
        def __init__(self,
                     workbook=None,
                     name="",
                     col_width=11.3,
                     format=None,
                     data_format=lambda x: x,
                     text_wrap=False,
                     excel_format_func=None,
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

            self.format = workbook.add_format(format) if format else None


    COLUMN_HEADERS = [ {"name": "PostId",
                        "col_width": 11.3,
                        "format": {"font_size": 9},
                        "data_format": lambda x: int(x)},
                       {"name": "Post",
                        "col_width": 5.5,
                        "format": {"font_size": 9,
                                   "underline": 1,
                                   "color": "blue"},
                        "data_format": lambda x: x},
                       {"name": "PostDate",
                        "col_width": 13.5,
                        "format": {"font_size": 9,
                                   "num_format": "mm/dd/yy hh:mm:ss" },
                        "data_format": lambda x: datetime.datetime.strptime(x, ExcelWriter.DEFAULT_DATE_FORMAT)
                       },
                       {"name": "PostMessage",
                        "col_width": 50,
                        "format": None,
                        "text_wrap": True,
                        "data_format": lambda x: x },
                       {"name": "ReplyDate",
                        "col_width": 13.5,
                        "format": {"font_size": 9,
                                   "num_format": "mm/dd/yy hh:mm:ss" },
                        "data_format": lambda x: datetime.datetime.strptime(x, ExcelWriter.DEFAULT_DATE_FORMAT)
                       },
                       {"name": "ReplyMessage",
                        "col_width": 50,
                        "format": None,
                        "text_wrap": True,
                        "data_format": lambda x: x},
                       {"name": "GOS",
                        "col_width": 4,
                        "format": None,
                        "data_format": lambda x: float(x)}]

    def __init__(self):
        self._ws = {}

    def create_workbook(self, name):

        if ".xlsx" not in name:
            name = name + ".xlsx"

        self._wb = xlsxwriter.Workbook(name)

        post_id = ExcelWriter.ColumnFormat(workbook=self._wb,
                                           name="PostID",
                                           col_width=11.3,
                                           format={"font_size": 9},
                                           data_format=lambda x: int(x),
                                           excel_format_func="write_number")

        post = ExcelWriter.ColumnFormat(workbook=self._wb,
                                        name="Post",
                                        col_width=5.5,
                                        format={"font_size": 9,
                                                "underline": 1,
                                                "color": "blue"},
                                        excel_format_func="write_url",
                                        excel_format_func_args="LINK")

        post_date = ExcelWriter.ColumnFormat(workbook=self._wb,
                                             name="PostDate",
                                             col_width=11.3,
                                             format={"font_size": 9,
                                                     "num_format": "mm/dd/yy hh:mm:ss"},
                                             data_format=lambda x: datetime.datetime.strptime(x, ExcelWriter.DEFAULT_DATE_FORMAT),
                                             excel_format_func="write_datetime")

        post_message = ExcelWriter.ColumnFormat(workbook=self._wb,
                                                name="PostMessage",
                                                col_width=50,
                                                text_wrap=True,
                                                excel_format_func="write_string")

        # shallow class attribute copies
        reply_date = ExcelWriter.ColumnFormat()
        reply_date.__dict__.update(post_date.__dict__)
        reply_date.name = "ReplyDate"

        reply_message = ExcelWriter.ColumnFormat()
        reply_message.__dict__.update(post_message.__dict__)
        reply_message.name = "ReplyMessage"

        gos = ExcelWriter.ColumnFormat(workbook=self._wb,
                                       name="GOS",
                                       col_width=4,
                                       data_format=lambda x: float(x),
                                       excel_format_func="write_number")

        self._col_headers = [ post_id,
                              post,
                              post_date,
                              post_message,
                              reply_date,
                              reply_message,
                              gos]

    def add_sheet(self, name, data):

        ws = self._wb.add_worksheet(name)
        self._ws[name] = ws

        # only enumerate the headers for which we have columns of data
        for i, hdr in enumerate(self._col_headers[0: len(data[0])]):

            fmt = self._wb.add_format()

            #text wrap and date formats don't mess up headers, font size does so it needs to go on cells
            # also, cell-based formatting overwrites column in most cases!
            if hdr.text_wrap:
                fmt.set_text_wrap()

            # set column width
            ws.set_column(i, i, hdr.col_width, fmt)

        self._add_table_to_worksheet(data, name)

        return ws

    def close_workbook(self):
        """
        MUST BE CALLED! Behavior indeterminate if it's not.
        :return: None
        """
        self._wb.close()

    def _add_data_to_table(self, ws, col_count, row_count, data):

            #write the data to each row
        for row_num, row in enumerate(data):
            # we need to start the row number at 1 for Excel, there is no row 0
            rn = row_num + 1

            # for each row, write out the data in each cell
            for col_num in xrange(col_count):  # iterate over columns
                hdr = self._col_headers[col_num] # get the header for this column

                # dynamic method call: we store the name of the formatting method we need from the Worksheet
                # class in the header objects. This allows us to get the instance of those methods by name so we
                # can call them dynamically.
                excel_cell_write_fn = getattr(ws, hdr.excel_format_func)

                # build function arguments - doing it this way so we can conditionally add the final argument if present
                arg_tuple = (rn,
                             col_num,
                             hdr.data_format(row[col_num]), # call the lambda to format this cell
                             hdr.format) # the format dict for the Excel library

                if hdr.excel_format_func_args:
                    # any add'l args to pass to the write fn will be appended in a singleton tuple (note the
                    # trailing comma - that's critical.) Since tuples are immutable you must assign back to the orig
                    # one to update it
                    arg_tuple = arg_tuple + (hdr.excel_format_func_args,)

                # writes the data to the cell
                excel_cell_write_fn(*arg_tuple) # unpack the tuple into arguments using nifty syntax



    def _add_table_to_worksheet(self, data, name):
        """
        adds an Excel table to a worksheet
        :param data: raw data to add - just used to calculate dimensions
        :param name: name of worksheet on which to put table
        :return: None
        """

        col_count = len(data[0])
        options = self._build_table_options(col_count, name)  # build options object to pass to API
        row_count = len(data) + 1  # have to add one for the header row
        end_col_letter = chr(col_count + ord('A') - 1)  # get the final column letter (easier for debugging)
        tbl_range = "A1:{}{:d}".format(end_col_letter, row_count)  # build the table range in Excel letter notation
        ws = self[name]
        hdr = self.COLUMN_HEADERS[0:col_count]
        ws.add_table(tbl_range, options)  # add the table to the worksheet

        self._add_data_to_table(ws, col_count, row_count, data)

        return

        #write the data to each row
        for row_num, row in enumerate(data):
            # we need to start the row number at 1 for Excel, there is no row 0
            rn = row_num + 1
            # either need a format function (or lambda) for each of these or I have to hard-code logic to
            # handle differing column lengths
            ws.write_number(rn, 0, int(row[0]), hdr[0]["format"])
            ws.write_url(rn, 1, row[1], hdr[1]["format"], "LINK")
            ws.write_datetime(rn, 2, datetime.datetime.strptime(row[2], self.DEFAULT_DATE_FORMAT), hdr[2]["format"])
            ws.write_string(rn, 3, row[3], hdr[3]["format"])
            if col_count > 4: # I THINK THIS SUCKS
                ws.write_datetime(rn, 4, datetime.datetime.strptime(row[4], self.DEFAULT_DATE_FORMAT),  hdr[4]["format"])
                ws.write_string(rn, 5, row[5], hdr[5]["format"])
                ws.write_number(rn, 6, float(row[6]),  hdr[6]["format"])

    def write_twitter_gos_data_for(self, name, data):
        """
        sets column headers, column width, and adds the table of data to the spreadsheet
        :param name: name of sheet to write to
        :param headers: list of headers in order
        :param data: raw data to write to rows
        :return: None
        """
        if name not in self._ws.keys():
            raise ValueError("sheet % not found" % name)
        else:
            """
            for i, header in enumerate(headers):
                # need to explicitly do this because you can't call str(x) on a unicode string or you'll get
                # an encoding exception. It won't harm unicode already-encoded strings (all the text already) and will
                # convert all other strings to utf8 strings to get their length
                encoded_text_for_col_width = unicode(data[0][i]).encode('utf-8')

                self._ws[name].set_column(i, i, len(encoded_text_for_col_width)) # set column widths

            self._add_table_to_worksheet(data, headers, name)


            for i, hdr in enumerate(self.COLUMN_HEADERS[0:len(data[0])]):
                #hdr = self.COLUMN_HEADERS[i]
                width = hdr["col_width"]
                fmt = self._wb.add_format()

                #text wrap and date formats don't mess up headers, font size does so it needs to go on cells
                # also, cell-based formatting overwrites column in most cases!
                if "text_wrap" in hdr:
                    fmt.set_text_wrap()
                if "date_format" in hdr:
                    fmt.set_num_format(hdr["date_format"])
                self._ws[name].set_column(i, i, width, fmt) # set column widths

            self._add_table_to_worksheet(data, name)
            """
            pass

    def _build_table_options(self, length, name):

        # create a dict in the format required by Excel lib. We only want headers for the columns of
        # data we have, thus the slice
        """
        Builds options object for creating a table using the Excel library
        :param length: length of headers required (6 for first/support touch ,4 for unanswered)
        :param name: name of sheet; used to name table
        :return: options object
        """
        #header_row = [{'header': hdr["name"]} for hdr in self.COLUMN_HEADERS[0:length]]

        header_row = [{'header': hdr.name} for hdr in self._col_headers[0:length]]

        #for h in headers:
        #    header_row.append({
        #        'header': h
        #    })

        options = {
            'style': 'Table Style Light 9', # med blue background headers with white bold text, no banding, row lines
            'columns': header_row,
            'name': "tbl" + name.translate(None, '- ')
        }
        return options

    def __getitem__(self, item):
        """
        indexer for this class
        :param item: name of worksheet
        :return: worksheet object specified by name
        """
        if not isinstance(item, str):
            raise ValueError("sheet name required")
        else:
            return self._ws[item]


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

        #for h in headers:
        #    header_row.append({
        #        'header': h
        #    })

        options = {
            'style': 'Table Style Light 9', # med blue background headers with white bold text, no banding, row lines
            'columns': header_row,
            'name': "tbl"
        }
        tbl_range = "A1:C2"

        ws.add_table(tbl_range, options)

        ws.write_number(1, 0, int("3"), wb.add_format({"font_size": 9}))

        wb.close()


