from rest_framework.parsers import FileUploadParser


class NoNameFileUploadParser(FileUploadParser):
    def get_filename(self, stream, media_type, parser_context):
        return 'no-filename'
