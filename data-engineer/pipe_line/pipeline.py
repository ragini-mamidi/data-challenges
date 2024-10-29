from xml_processor import XMLProcessor

DIR = '/data-engineer/data'
DB = 'my_database.db'
TABLE = 'events'


processor = XMLProcessor(DIR, DB, TABLE)
processor.read_directory()
processor.load_xml_to_sqlite()
processor.window_by_datetime('1D')
ro_records = processor.process_to_RO()

for record in ro_records:
    print(record)