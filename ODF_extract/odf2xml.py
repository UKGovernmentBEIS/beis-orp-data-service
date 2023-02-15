import sys, getopt, base64
from odf.opendocument import load
from odf.draw import Image, ObjectOle
from odf.style import BackgroundImage
from odf.text import ListLevelStyleImage
from odf.office import BinaryData

# if sys.version_info[0]==3: unicode=str

# def usage():
#    sys.stderr.write("Usage: %s [-e] [-o outputfile] [inputfile]\n" % sys.argv[0])


def write_xml():
    embedimage = False
    # try:
    #     opts, args = getopt.getopt(sys.argv[1:], "o:e", ["output="])
    # except getopt.GetoptError:

    # outputfile = '-'

    # for o, a in opts:
    #     if o in ("-o", "--output"):
    #         outputfile = a
    #     if o == '-e':
    #         embedimage = True

    # if len(args) > 1:
    # if len(args) == 0:
    d = load("/Users/thomas/Documents/BEIS/input_data/ODF/OpenDocument-v1.2-os.odt")
    # else:
    #     d = load(unicode(args[0]))
    if embedimage:
        images = d.getElementsByType(Image) + \
           d.getElementsByType(BackgroundImage) +  \
           d.getElementsByType(ObjectOle) + \
           d.getElementsByType(ListLevelStyleImage)
        for image in images:
            href = image.getAttribute('href')
            if href and href[:9] == "Pictures/":
                p = d.Pictures[href]
                bp = base64.encodestring(p[1])
                image.addElement(BinaryData(text=bp))
                image.removeAttribute('href')
    xml = d.xml()
    if outputfile == '-':
       print (xml)
    else:
        open(outputfile,"wb").write(xml)