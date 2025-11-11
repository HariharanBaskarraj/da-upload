from django.contrib import admin
from django.urls import path
from da_processor.views import (
    DistributionAuthorizationAPIView,
    LicenseeDefaultsAPIView,
    HealthCheckView
)

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/health/', HealthCheckView.as_view(), name='health-check'),
    path('api/v1/distribution-authorization/', DistributionAuthorizationAPIView.as_view(), name='da-create'),
    path('api/v1/licensee/<str:licensee_id>/defaults/', LicenseeDefaultsAPIView.as_view(), name='licensee-defaults'),
]