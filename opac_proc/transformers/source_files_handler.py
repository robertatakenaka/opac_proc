# coding: utf-8

import os

from opac_proc.web import config
from . import html_generator

"""
Identify the fulltexts locations from xylose, and create the structure:
    xylose:
        For "html"
        "fulltexts": {
            "pdf": {
                "en": "http://www.scielo.br/pdf/rsp/v40n3/en_07.pdf",
                "pt": "http://www.scielo.br/pdf/rsp/v40n3/07.pdf"
                },
            "html": {
                "en": "http://www.scielo.br/scielo.php?script=sci_arttext&pid=S0034-89102006000300007&tlng=en",
                "pt": "http://www.scielo.br/scielo.php?script=sci_arttext&pid=S0034-89102006000300007&tlng=pt"
            }
        }
        For XML:
        data_model_version == 'xml'
        build the pdf url from xml_languages
"""

class SourceTextFile(object):

    def __init__(self, source_location):
        self.source_location = source_location
        self.path = os.path.dirname(source_location)
        self.filename = os.path.basename(source_location)
        self.name, self.ext = os.path.splitext(self.filename)

    @property
    def location(self):
        if os.path.isfile(self.source_location):
            return self.source_location

    @property
    def pfile(self):
        if self.location is not None:
            try:
                _pfile = open(self.location, 'rb')
                return _pfile
            except Exception, e:            
                pass


class SourceFiles(object):

    def __init__(self, xylose_article, css):
        self.xylose_article = xylose_article
        self.issue_folder_name = self.xylose_article.assets_code
        self.journal_folder_name = self.xylose_article.journal.acronym.lower()
        self.article_folder_name = self.xylose_article.file_code()
        self.css = css
        self._generated_html_items = None
        self.generated_html_errors = None
        self.setUp()

    def setUp(self):
        self._texts_info = self._get_data_from_sgm_version()
        self._texts_info.update(self._get_data_from_sps_version())
        
    @property
    def bucket_name(self):
        return '-'.join([self.journal_folder_name, self.issue_folder_name, self.article_folder_name])

    @property
    def issue_folder_rel_path(self):
        return '/'.join([self.journal_folder_name, self.issue_folder_name])

    @property
    def article_metadata(self):
        metadata = {}
        metadata['article-folder'] = self.article_folder_name
        metadata['issue-folder'] = self.issue_folder_name
        metadata['journal-folder'] = self.journal_folder_name
        return metadata

    @property
    def pdf_files(self):
        return self._texts_info.get('pdf', {})

    @property
    def pdf_folder_path(self):
        return '/'.join([config.OPAC_PROC_ASSETS_SOURCE_PDF_PATH, self.issue_folder_rel_path])

    @property
    def media_folder_path(self):
        return '/'.join([config.OPAC_PROC_ASSETS_SOURCE_MEDIA_PATH, self.issue_folder_rel_path])

    @property
    def xml_folder_path(self):
        return '/'.join([config.OPAC_PROC_ASSETS_SOURCE_XML_PATH, self.issue_folder_rel_path])

    def _get_data_from_sgm_version(self):
        fulltext_files = {}
        if hasattr(self.xylose_article, 'fulltexts'):
            pdf_url_items = self.xylose_article.fulltexts().get('pdf')
            fulltext_files['pdf'] = {}
            if pdf_url_items is not None:
                for lang in pdf_url_items.keys():
                    prefix = '' if lang != self.xylose_article.original_language else lang+'_'
                    fulltext_files['pdf'][lang] = SourceTextFile('{}/{}{}.pdf'.format(self.pdf_folder_path, prefix, self.article_folder_name))
        return fulltext_files 

    def _get_data_from_sps_version(self):
        fulltext_files = {}
        if self.xylose_article.data_model_version == 'xml':
            fulltext_files['pdf'] = {}
            if hasattr(self.xylose_article, 'xml_languages'):
                if self.xylose_article.xml_languages() is not None:
                    for lang in self.xylose_article.xml_languages():
                        prefix = '' if lang == self.xylose_article.original_language else lang+'_'

                        fulltext_files['pdf'][lang] = SourceTextFile('{}/{}{}.pdf'.format(self.pdf_folder_path, prefix, self.article_folder_name))
        return fulltext_files

    @property
    def media_files(self):
        files = {}
        for path in [self.media_folder_path, self.media_folder_path + '/html']:    
            if os.path.isdir(path):
                files.update({fname: SourceTextFile(path + '/' + fname) for fname in os.listdir(path) if fname.startswith(self.article_folder_name)})
        return files

    @property
    def xml_file(self):
        if self.xylose_article.data_model_version == 'xml':
            return SourceTextFile(self.xml_folder_path + '/' + self.article_folder_name + '.xml')

    def generate_htmls(self):
        self._generated_html_items = None
        self.generated_html_errors = None
        if self.xml_file is not None:
            files, errors = html_generator.generate_html(self.xml_file.location, self.css)
            self._generated_html_items = files
            self.generated_html_errors = errors

    @property
    def generated_html_items(self):
        if self._generated_html_items is None:
            self.generate_htmls()
        return self._generated_html_items

    def generated_html_files(self, replacements=None):
        result = self.generated_html_items
        if result is not None:
            result = {}
            for lang, content in self.generated_html_items.items():
                if replacements is not None:
                    for media_name, url in replacements.items():
                        href_content = 'href="{}"'.format(media_name.replace('-DOT-', '.'))
                        ssm_href_content = 'href="{}"'.format(url)
                        content = content.replace(href_content, ssm_href_content)
                try:
                    result[lang] = StringIO.StringIO(content.encode('utf-8'))
                except:
                    if self.generated_html_errors is None:
                        self.generated_html_errors = []
                    self.generated_html_errors.append(u'Não foi possível gerar pfile correspondente a {} {}'.format('html', lang))
        return result
                    