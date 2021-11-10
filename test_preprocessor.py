import xlrd

import preprocessor
import unittest
import os

os.environ["table"] = "table"
os.environ["region"] = "region"
os.environ["provider_name_from_s3_folder"] = "walterpresents"


def generate_test_row_data():
    test_row = {
        'Provider External ID': 'imperial_101',
        'Title': 'Episode 1',
        'Description': 'Test Description',
        'Episode Number': '1',
        'Season Number': '1',
        'Closed Captioning': True,
        'Format': 'HD',
        'Release Year': '2015',
        'Asset Length': '3149',
        'Original Language': 'en',
        'Tags': 'test, tags, imperial, italian, italy, hotel, drama, murder, mystery, romance, privilege, maids, '
                'managers, money, rich',
        'Images': {
            'X2_Gallery_Image_1': 'globalseries-imperial_0101-4x3-it-IT.jpg'
        },
        'Video': 'globalseries-Imperial_101-Full-Mezz_HD-it-IT.mp4',
        'License Start': '2020-07-03',
        'License End': '2023-06-30'
        }

    return test_row


class Test_Preprocessor(unittest.TestCase):
    def test_preprocessor_sanity_check(self):
        p = preprocessor.Preprocessor()
        self.assertTrue(p)

    def test_preprocessor_parse_row_into_db_dict_good(self):
        p = preprocessor.Preprocessor()
        input_file = "../test/comcast-imperial-single-entry.xls"
        workbook = xlrd.open_workbook(input_file)
        worksheet = workbook.sheet_by_index(0)
        row = worksheet.row_values(1)
        db_dict = p.parse_row_into_db_dict(row)
        test_row_data = generate_test_row_data()
        self.assertTrue(db_dict == test_row_data)

    def test_convert_datetime_string_ok1_format(self):
        teststring = "2020-07-03T00:00:00ZT09:00+00"
        ansstring = "2020-07-03"
        p = preprocessor.Preprocessor()
        resstring = p.convert_datetime_string(teststring)
        self.assertTrue(resstring == ansstring)

    def test_convert_datetime_string_ok2_format(self):
        teststring = "2020-07-03T00:00:00Z"
        ansstring = "2020-07-03"
        p = preprocessor.Preprocessor()
        resstring = p.convert_datetime_string(teststring)
        self.assertTrue(resstring == ansstring)

    def test_convert_datetime_string_ok2_format(self):
        teststring = "2020-07-03"
        ansstring = "2020-07-03"
        p = preprocessor.Preprocessor()
        resstring = p.convert_datetime_string(teststring)
        self.assertTrue(resstring == ansstring)

    def test_convert_episode_id_to_series_name_ok1(self):
        teststring = "imperial_101"
        ansstring = "Imperial"
        p = preprocessor.Preprocessor()
        resstring = p.convert_provider_external_id_to_series_name(teststring)
        self.assertTrue(resstring == ansstring)

    def test_convert_episode_id_to_series_name_ok2(self):
        teststring = "fatale_station_101"
        ansstring = "Fatale Station"
        p = preprocessor.Preprocessor()
        resstring = p.convert_provider_external_id_to_series_name(teststring)
        self.assertTrue(resstring == ansstring)


if __name__ == "__main__":
    unittest.main()
