import lxml.etree as ET
import os
import argparse


def main():
    parser = argparse.ArgumentParser(
        description='This tool transforms an XML of ISO20022-compliant financial instrument reference data to a CSV output of selected relevant fields. The XSL files are provided for both FULINS and DLTINS schemas. Output file is produced alongside input XML file if no destination path is provided as argument.')
    requiredNamed = parser.add_argument_group('required named arguments')
    requiredNamed.add_argument('--xml', type=str, help='The input XML file to be transformed.', required=True)
    requiredNamed.add_argument('--xsl', type=str, help='The input XSL file to perform the transformation.', required=True)
    parser.add_argument('--out', type=str, help='The output CSV file destination.')
    args = parser.parse_args()

    # Variable declarations
    xmlFullPath = os.path.expanduser(args.xml)
    xslFullPath = os.path.expanduser(args.xsl)
    # TODO: check if files exist

    # POINT TO OUTPUT FILE
    csvFullPath = os.path.expanduser(xmlFullPath[:-3] + 'csv') if args.out is None else args.out
    transform(xmlFullPath, xslFullPath, csvFullPath)


def transform(xmlFullPath, xslFullPath, csvFullPath):
    # LOAD XML AND XSL SOURCES
    xml = ET.parse(xmlFullPath)
    print("Finished parsing XML.")
    xsl = ET.parse(xslFullPath)
    print("Finished parsing XSL.")

    # TRANSFORM SOURCE
    transform = ET.XSLT(xsl)
    print("Finished transforming XSL.")

    newDOM = transform(xml)
    print("Finished transforming XML.")

    # SAVE AS .CSV
    with open(csvFullPath, 'wb') as f:
        f.write(newDOM)
        f.close()


if __name__ == "__main__":
    main()
