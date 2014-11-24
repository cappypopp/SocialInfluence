__author__ = 'cappy'

import xlsxwriter,datetime

class ExcelWriter(object):

    _wb = None

    _ws = {}

    _data = {}

    DEFAULT_DATE_FORMAT = "%m/%d/%Y %I:%M:%S %p"

    COLUMN_HEADERS = [ {"name": "PostId",
                        "col_width": 11.3,
                        "format": {"font_size": 9}},
                       {"name": "Post",
                        "col_width": 5.5,
                        "format": {"font_size": 9,
                                   "underline": 1,
                                   "color": "blue"}},
                       {"name": "PostDate",
                        "col_width": 13.5,
                        "format": {"font_size": 9,
                                   "num_format": "mm/dd/yy hh:mm:ss" },
                       },
                       {"name": "PostMessage",
                        "col_width": 50,
                        "format": None,
                        "text_wrap": True },
                       {"name": "ReplyDate",
                        "col_width": 13.5,
                        "format": {"font_size": 9,
                                   "num_format": "mm/dd/yy hh:mm:ss" },
                       },
                       {"name": "ReplyMessage",
                        "col_width": 50,
                        "format": None,
                        "text_wrap": True},
                       {"name": "GOS",
                        "col_width": 4,
                        "format": None}]

    def __init__(self):
        pass

    def create_workbook(self, name):
        if ".xlsx" not in name:
            name = name + ".xlsx"
        self._wb = xlsxwriter.Workbook(name)
        
        # add 9-point font size format
        font_size_9pt = self._wb.add_format()
        font_size_9pt.set_font_size(9)

        for hdr in self.COLUMN_HEADERS:
            if hdr["format"]:
                hdr["format"] = self._wb.add_format(hdr['format'])

    def add_sheet(self, name):
        ws = self._wb.add_worksheet(name)
        self._ws[name] = ws
        return ws

    def close_workbook(self):
        """
        MUST BE CALLED! Behavior indeterminate if it's not.
        :return: None
        """
        self._wb.close()


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

        #write the data to each row
        for row_num, row in enumerate(data):
            # we need to start the row number at 1 for Excel, there is no row 0
            rn = row_num + 1
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
            """
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


    def _build_table_options(self, length, name):

        # create a dict in the format required by Excel lib. We only want headers for the columns of
        # data we have, thus the slice
        """
        Builds options object for creating a table using the Excel library
        :param length: length of headers required (6 for first/support touch ,4 for unanswered)
        :param name: name of sheet; used to name table
        :return: options object
        """
        header_row = [{'header': hdr["name"]} for hdr in self.COLUMN_HEADERS[0:length]]

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
