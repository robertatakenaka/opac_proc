# code = utf-8
import packtools

from lxml import etree

class XMLError(Exception):
    """ Represents errors that would block HTMLGenerator instance from
    being created.
    """


def get_htmlgenerator(xmlpath, no_network, no_checks, css):
    try:
        parsed_xml = packtools.XML(xmlpath, no_network=no_network)
    except IOError as e:
        raise XMLError('Error reading %s. Make sure it is a valid file-path or URL.' % xmlpath)
    except etree.XMLSyntaxError as e:
        raise XMLError('Error reading %s. Syntax error: %s' % (xmlpath, e))

    try:
        generator = packtools.HTMLGenerator.parse(parsed_xml, valid_only=not no_checks, css=css)
    except ValueError as e:
        raise XMLError('Error reading %s. %s.' % (xmlpath, e))

    return generator


def generate_html(xml, css):
    error_message = []
    files = {}
    html_generator = None
    try:
        html_generator = get_htmlgenerator(xml, False, True, css)
    except XMLError as e:
        error_message = 'Error generating {}. '.format(xml)

    if html_generator is not None:
        try:
            for lang, trans_result in html_generator:
                files[lang] = etree.tostring(trans_result, pretty_print=True,
                                            encoding='utf-8', method='html',
                                            doctype=u"<!DOCTYPE html>")
                files[lang] = files[lang].decode('utf-8')

        except TypeError as e:
            error_message = 'Error generating html ({}) for {}. '.format(lang, xml)
        except:
            error_message = 'Unknown Error generating html ({}) for {}. '.format(lang, xml)

    if len(files) > 0:
        return (True, files)
    else:
        return (False, error_message)
    