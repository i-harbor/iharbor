class ServerHeaderMiddleware:
    """
    避免header Server泄露信息
    default header Server: WSGIServer/x.x CPython/3.x.x
    """
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)
        response['Server'] = 'iHarborS3'
        return response
