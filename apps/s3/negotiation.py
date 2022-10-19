from rest_framework.negotiation import DefaultContentNegotiation
from s3.parsers import S3XMLParser


class CusContentNegotiation(DefaultContentNegotiation):
    def select_parser(self, request, parsers):
        """
        Given a list of parsers and a media type, return the appropriate
        parser to handle the incoming request.
        """
        parser = super().select_parser(request=request, parsers=parsers)
        if parser:
            return parser

        return S3XMLParser()

