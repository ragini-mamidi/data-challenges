import unittest
import os
import pandas as pd
import json
from xml_processor import XMLProcessor  # Adjust the import based on your actual module name

class TestXMLProcessor(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up a temporary directory and XMLProcessor instance for testing."""
        cls.test_dir = 'test_data'
        os.makedirs(cls.test_dir, exist_ok=True)
        
        # Create test XML files
        cls.test_files = [
            'test1.xml',
            'test2.xml'
        ]
        cls.test_xml_content = [
            """<event>
<order_id>101</order_id>
    <date_time>2023-08-10T10:00:00</date_time>
    <status>In Progress</status>
    <cost>50.25</cost>
    <repair_details>
        <technician>Jane Smith</technician>
            <repair_parts>
                <part name="Air Filter" quantity="1"/>
            </repair_parts>
    </repair_details>
</event>""",
            """<event>
    <order_id>102</order_id>
    <date_time>2023-08-11T12:00:00</date_time>
    <status>Completed</status>
    <cost>75.00</cost>
    <repair_details>
        <technician>John Doe</technician>
            <repair_parts>
                <part name="Oil Filter" quantity="2"/>
            </repair_parts>
    </repair_details>
</event>"""
        ]

        for file_name, content in zip(cls.test_files, cls.test_xml_content):
            with open(os.path.join(cls.test_dir, file_name), 'w') as f:
                f.write(content)

        cls.processor = XMLProcessor(cls.test_dir, 'test.db', 'test_table')

    @classmethod
    def tearDownClass(cls):
        """Clean up the temporary directory after tests."""
        for file_name in cls.test_files:
            os.remove(os.path.join(cls.test_dir, file_name))
        os.rmdir(cls.test_dir)

    def test_read_directory(self):
        """Test reading XML files from the directory."""
        files = self.processor.read_directory()
        self.assertEqual(len(files), 2)
        self.assertIn('test1.xml', files)
        self.assertIn('test2.xml', files)

    def test_flatten_event(self):
        """Test flattening an XML event."""
        event_data = self.test_xml_content[0]
        filename = 'test1.xml'
        flattened_record = self.processor.flatten_event(event_data, filename)
        self.assertEqual(flattened_record['order_id'], '101')
        self.assertEqual(flattened_record['status'], 'In Progress')
        self.assertEqual(flattened_record['technician'], 'Jane Smith')
        self.assertIn('repair_parts', flattened_record)

    def test_process_xml_files(self):
        """Test processing of XML files."""
        self.processor.read_directory()
        _, bad_df = self.processor.process_xml_files()        
        self.assertEqual(len(bad_df), 0)  # Assuming no bad files

    def test_window_by_datetime(self):
        """Test windowing by datetime."""
        self.processor.read_directory()
        self.processor.process_xml_files()  # Ensure we have data processed
        windowed_data = self.processor.window_by_datetime('1D')
        self.assertEqual(len(windowed_data), 2)  # Expecting 2 windows based on test data

    def test_process_to_RO(self):
        """Test transforming windowed data into structured RO format."""
        self.processor.read_directory()
        self.processor.process_xml_files()
        self.processor.window_by_datetime('1D')
        ro_list = self.processor.process_to_RO()
        self.assertEqual(len(ro_list), 2)  # Expecting 2 RO records

if __name__ == '__main__':
    unittest.main()
