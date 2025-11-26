from django.urls import path
from drf_spectacular.views import (
    SpectacularAPIView,
    SpectacularSwaggerView,
    SpectacularRedocView
)
from da_processor.views import (
    DistributionAuthorizationAPIView,
    HealthCheckView
)

urlpatterns = [
    # API Endpoints
    path('api/health/', HealthCheckView.as_view(), name='health-check'),
    path('api/v1/distribution-authorization/', DistributionAuthorizationAPIView.as_view(), name='da-create'),

    # OpenAPI schema
    path('api/schema/', SpectacularAPIView.as_view(), name='schema'),
    
    # Swagger UI
    path('api/docs/', SpectacularSwaggerView.as_view(url_name='schema'), name='swagger-ui'),
    
    # ReDoc UI
    path('api/redoc/', SpectacularRedocView.as_view(url_name='schema'), name='redoc')
]