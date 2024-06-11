import os
from dotenv import load_dotenv
from docopt import docopt
from utils import file_utils
from excel_transformer import ExcelTransformer


def main():

    """Merkkisiivous

    Usage:
        merkkisiivous.exe [--merkkisuunnitelmat=DIR]
        merkkisiivous.exe --help

    Options:
        -h, --help                       Tämä teksti
        -m DIR --merkkisuunnitelmat=DIR  Kansio josta merkkisuunnitelmia luetaan [default: merkkisuunnitelmat]
    """

    arguments = docopt(main.__doc__)
    signplan_dir = arguments['--merkkisuunnitelmat']
    signplan_dir = os.path.join(os.getcwd(),signplan_dir)

    files = file_utils.read_files(signplan_dir)
    if len(files) == 0:
        print(f"Polusta {signplan_dir} tai sen alikansioista ei löytynyt merkkisuunnitelmia ja/tai ratko-dataa.")
        return
    
    transformer = ExcelTransformer(files[0]["ratko"].columns.tolist()) # Columns are same for all files.
    for f in files:
        ratko_data = f["ratko"]
        signplans = f["signplans"]

        for plan in signplans:
            transformer.initialize(plan,ratko_data)
            transformer.transform_excel()

if __name__ == "__main__":
    load_dotenv()
    main()