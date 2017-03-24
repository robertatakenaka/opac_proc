# coding: utf-8

import os

from opac_proc.web import config
from opac_proc.transformers import html_generator
from opac_proc.core.asset_handler import AssetHandler


class ArticleSourceFiles(object):
    """
    ArticleSourceFiles
    PDF, XML, Media, HTML files
    """
    def __init__(self, xylose_article):
        self.xylose_article = xylose_article
        self.issue_folder_name = self.xylose_article.assets_code
        self.journal_folder_name = self.xylose_article.journal.acronym.lower()
        self.article_folder_name = self.xylose_article.file_code()
        
    @property
    def bucket_name(self):
        return '-'.join([self.journal_folder_name, self.issue_folder_name, self.article_folder_name])

    @property
    def issue_folder_rel_path(self):
        return '/'.join([self.journal_folder_name, self.issue_folder_name])

    @property
    def article_metadata(self):
        metadata = {}
        metadata['article_folder'] = self.article_folder_name
        metadata['issue_folder'] = self.issue_folder_name
        metadata['journal_folder'] = self.journal_folder_name
        metadata['bucket_name'] = self.bucket_name
        metadata['article_pid'] = self.xylose_article.publisher_id

        return metadata

    @property
    def pdf_folder_path(self):
        return '/'.join([config.OPAC_PROC_ASSETS_SOURCE_PDF_PATH, self.issue_folder_rel_path])

    @property
    def media_folder_path(self):
        return '/'.join([config.OPAC_PROC_ASSETS_SOURCE_MEDIA_PATH, self.issue_folder_rel_path])

    @property
    def xml_folder_path(self):
        return '/'.join([config.OPAC_PROC_ASSETS_SOURCE_XML_PATH, self.issue_folder_rel_path])

    @property
    def pdf_files(self):
        fulltext_files = {}
        langs = []
        if hasattr(self.xylose_article, 'fulltexts'):
            langs.extend(self.xylose_article.fulltexts().get('pdf', {}).keys())
        elif self.xylose_article.data_model_version == 'xml':
            langs.extend(self.xylose_article.xml_languages())
        for lang in langs:
            prefix = '' if lang != self.xylose_article.original_language else lang+'_'
            fulltext_files[lang] = '{}/{}{}.pdf'.format(self.pdf_folder_path, prefix, self.article_folder_name)
        return fulltext_files 

    @property
    def media_files(self):
        files = {}
        for path in [self.media_folder_path, self.media_folder_path + '/html']:    
            if os.path.isdir(path):
                files.update({fname: path + '/' + fname for fname in os.listdir(path) if fname.startswith(self.article_folder_name)})
        return files

    @property
    def xml_file(self):
        if self.xylose_article.data_model_version == 'xml':
            return self.xml_folder_path + '/' + self.article_folder_name + '.xml'


class Assets(object):

    def __init__(self, article_uuid, xylose_article, css_path):
        self.css_path = css_path
        self.source_files = ArticleSourceFiles(xylose_article)
        self.assets_errors = []
        self.article_uuid = article_uuid
        self.article_metadata = self.source_files.article_metadata.copy()
        self.article_metadata.update({
            'article-uuid': article_uuid,
            })
        self.generate_pdf_assets()
        self.generate_media_assets()
        self.generate_xml_assets()

    def pfile(self, filename):
        try:
            _pfile = open(filename, 'rb')
        except Exception, e:            
            pass
        else:
            return _pfile
            
    @property
    def assets_sources(self):
        sources = {}
        sources['pdfs'] = self.source_files.pdf_files.values()
        sources['media'] = self.source_files.media_files.values()
        sources['xml'] = self.source_files.xml_file
        return sources

    def generate_pdf_assets(self):
        self.pdf_assets = []
        for lang, source_file in self.source_files.pdf_files.items():
            file_metadata = {'lang': lang}
            file_metadata.update(self.article_metadata)
            
            pfile = self.pfile(source_file)
            if pfile is not None:
                self.pdf_assets.append(AssetHandler(pfile, os.path.basename(source_file), 'pdf', file_metadata, self.source_files.bucket_name))
            else:
                msg = u'Não foi possível ler o arquivo {} correspondente a {} {} '.format(source_file, 'pdf', lang)
                self.assets_errors.append(msg)

    def generate_media_assets(self):
        self.media_assets = []
        for href, source_file in self.source_files.media_files.items():
            file_metadata = {'href': href}
            file_metadata.update(self.article_metadata)
            
            pfile = self.pfile(source_file)
            if pfile is not None:
                self.media_assets.append(AssetHandler(pfile, os.path.basename(source_file), '', file_metadata, self.source_files.bucket_name))
            else:
                msg = u'Não foi possível ler o arquivo {} correspondente a {} {} '.format(source_file, 'media', href)
                self.assets_errors.append(msg)

    def generate_xml_assets(self):
        self.xml_assets = []
        source_file = self.source_files.xml_file
        if source_file is not None:
            file_metadata = {'label': 'xml'}
            file_metadata.update(self.article_metadata)
            pfile = self.pfile(source_file)
            if pfile is not None:
                self.xml_assets.append(AssetHandler(pfile, os.path.basename(source_file), 'xml', file_metadata, self.source_files.bucket_name))
            else:
                msg = u'Não foi possível ler o arquivo {} correspondente a {} {} '.format(source_file, 'media', href)
                self.assets_errors.append(msg)

    def register(self):
        for asset in [self.media_assets + self.xml_assets + self.pdf_assets]:
            asset.register_async()

    def executing(self, assets_items):
        return [asset.uuid for asset in assets_items if not asset.registration_status() in ('FAILURE', 'SUCCESS')]

    def waiting(self, assets_items):
        doing = self.executing(assets_items)
        while len(doing) > 0:
            self.assets_errors.append(u'Esperando o registro de {} ativos'.format(doing))
            doing = self.executing(assets_items)
            
    def registered_pdfs(self):
        data = []
        self.waiting(self.pdf_assets)
        for asset in self.pdf_assets:
            if asset.registration_status() == 'SUCCESS':
                data.append({'type': asset.filetype, 
                        'language': asset.metadata.get('lang'),
                        'url': asset.get_urls().get('url')})
            elif asset.registration_status() == 'FAILURE':
                self.assets_errors.append(u'Falha ao registrar {} {} '.format(asset.uuid, asset.name))
        return data

    def registered_media_assets(self):
        self.waiting(self.media_assets)
        return {asset.name.replace('.', '-DOT-'): asset.url for asset in self.media_assets}

    def registered_htmls(self):
        data = []
        self.waiting(self.html_assets)
        for asset in self.html_assets:
            if asset.registration_status() == 'SUCCESS':
                data.append({'type': asset.filetype, 
                        'language': asset.metadata.get('lang'),
                        'url': asset.get_urls().get('url')})
            elif asset.registration_status() == 'FAILURE':
                self.assets_errors.append(u'Falha ao registrar {} {} '.format(asset.uuid, asset.name))

    def generate_html_assets(self):
        self.html_assets = []
        html_files = self.generated_html_files(self.registered_media_assets())
        
        for lang, pfile in html_files.items():
            filename = lang+'_'+self.source_files.article_folder_name + '.html'                        
            file_metadata = {'lang': lang}
            file_metadata.update(self.article_metadata)
            
            pfile = self.pfile(source_file)
            if pfile is not None:
                self.html_assets.append(AssetHandler(pfile, filename, 'html', file_metadata, self.source_files.bucket_name))
            else:
                msg = u'Não foi possível ler o arquivo {} correspondente a {} {} '.format(filename, 'pdf', lang)
                self.assets_errors.append(msg)

    def register_htmls_assets(self):
        for asset in self.html_assets:
            asset.register()
        self.assets_items.extend(self.html_assets)

    def generated_html_files(self, replacements=None):
        files, errors = html_generator.generate_html(self.source_files.xml_file, self.css_path)
        result = {}
        for lang, content in files.items():
            if replacements is not None:
                for media_name, url in replacements.items():
                    href_content = 'href="{}"'.format(media_name.replace('-DOT-', '.'))
                    ssm_href_content = 'href="{}"'.format(url)
                    content = content.replace(href_content, ssm_href_content)
            try:
                result[lang] = StringIO.StringIO(content.encode('utf-8'))
            except:
                self.assets_errors.extend(errors)
        return result
