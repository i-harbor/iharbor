from drf_yasg.app_settings import swagger_settings
from drf_yasg.inspectors import CoreAPICompatInspector, FieldInspector, NotHandled, SwaggerAutoSchema


class NoPagingAutoSchema(SwaggerAutoSchema):
    def should_page(self):
        return False

