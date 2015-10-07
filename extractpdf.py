#!/usr/bin/python
import multiprocessing
import logging


filename_filter = "*.pdf"
filename_output = "result.json"
max_processes = 2
output_lock = multiprocessing.Lock()
logging.getLogger().setLevel(logging.INFO)


def get_result_from_file(filename):
    from pdfminer.pdfparser import PDFParser
    from pdfminer.pdfdocument import PDFDocument
    from pdfminer.pdfpage import PDFPage
    from pdfminer.pdfpage import PDFTextExtractionNotAllowed
    from pdfminer.pdfinterp import PDFResourceManager
    from pdfminer.pdfinterp import PDFPageInterpreter
    from pdfminer.converter import PDFPageAggregator
    from pdfminer.layout import LAParams

    result = {"filename": filename, "pages": []}
    fp = open(filename, 'rb')
    parser = PDFParser(fp)
    document = PDFDocument(parser)
    if not document.is_extractable:
        raise PDFTextExtractionNotAllowed
    rsrcmgr = PDFResourceManager()
    laparams = LAParams()
    laparams.char_margin = 2.0
    laparams.detect_vertical = True
    laparams.line_margin = 1.0
    device = PDFPageAggregator(rsrcmgr, laparams=laparams)
    interpreter = PDFPageInterpreter(rsrcmgr, device)
    page_index = 0
    for page in PDFPage.create_pages(document):
        interpreter.process_page(page)
        layout = device.get_result()
        bounding_box = get_bounding_box(layout)
        labels = get_text_labels(layout)
        result["pages"].append({
            "index": page_index,
            "bounding_box": bounding_box,
            "labels": labels})
        page_index += 1
    fp.close()
    return result


def get_bounding_box(layout):
    bbox = layout.bbox
    bounding_box = {'x0': bbox[0], 'y0': bbox[1], 'x1': bbox[2], 'y1': bbox[3]}
    return bounding_box


def get_text_labels(layout):
    labels = []
    parse_obj(layout._objs, labels)
    return labels


def parse_obj(objs, labels):
    from pdfminer.layout import LTTextLine, LTTextBox, LTFigure

    for obj in objs:
        if isinstance(obj, LTTextLine):
            text = obj.get_text().strip()
            if len(text) > 0:
                fontname, fontsize, orientation = get_font(obj)
                add(labels, fontname, fontsize, obj, orientation, text)
        elif isinstance(obj, (LTFigure, LTTextBox)):
            parse_obj(obj._objs, labels)


def get_font(obj):
    from pdfminer.layout import LTTextLine, LTChar

    for obj in obj._objs:
        if isinstance(obj, LTChar):
            return obj.fontname, obj.fontsize, 'H' if obj.upright else 'V'
        elif isinstance(obj, LTTextLine):
            return get_font(obj)
    return 'unknown', 0, 'H'


def add(labels, fontname, fontsize, obj, orientation, text):
    labels.append({
        "x0": obj.x0,
        "y0": obj.y0,
        "x1": obj.x1,
        "y1": obj.y1,
        "fontname": fontname,
        "fontsize": fontsize,
        "orientation": orientation,
        "text": text})


def write_to_output(filename, labels):
    import json
    import os

    with output_lock:
        if os.path.isfile(filename):
            delim = ','
        else:
            delim = '['
        with open(filename, 'a+') as f:
            f.write(delim)
            f.write(json.dumps(labels, sort_keys=True, indent=4))


def process_queue(queue, filename):
    while True:
        filename_input = queue.get()
        logging.info('Processing %s.', filename_input)
        labels = get_result_from_file(filename_input)
        logging.debug('Writing %s.', filename_input)
        write_to_output(filename, labels)
        logging.debug('File %s is processed.', filename_input)
        queue.task_done()


def process_files():
    import os
    import fnmatch

    file_count = 0
    file_queue = multiprocessing.JoinableQueue()

    if os.path.isfile(filename_output):
        os.remove(filename_output)

    for i in range(max_processes):
        worker = multiprocessing.Process(target=process_queue, args=(file_queue, filename_output))
        worker.daemon = True
        worker.start()

    for root, folder, files in os.walk("."):
        for item in fnmatch.filter(files, filename_filter):
            filename = root + '\\' + item
            logging.debug("Adding %s to the queue.", filename)
            file_count += 1
            file_queue.put(filename)

    file_queue.join()

    with output_lock:
        with open(filename_output, 'a') as f:
            f.write(']')

    logging.info("%d files processed.", file_count)


def show_help():
    print "extractpdf [-h] [-p processes] [-o output] [filter]"
    print "\t-h\tThis helptext."
    print "\t-o\tOutput filename. Defaults to %s." % filename_output
    print "\t-p\tMaximum number of processes. Defaults to %d." % max_processes
    print "\tfilter\tOnly process filenames matching this pattern. Defaults to %s." % filename_filter
    print "\nExample: extractpdf -p 4 -o sample.json 00*.pdf"


if __name__ == '__main__':
    from getopt import getopt, GetoptError
    from sys import argv

    opts, args = [], []
    try:
        opts, args = getopt(argv[1:], "ho:p:")
    except GetoptError:
        print "Incorrect arguments."
        show_help()
        exit(2)
    for opt, arg in opts:
        if opt == "-o":
            filename_output = arg
        if opt == "-p":
            max_processes = int(arg)
        elif opt == "-h":
            show_help()
            exit()
    if len(args) > 0:
        filename_filter = args[0]
    process_files()
