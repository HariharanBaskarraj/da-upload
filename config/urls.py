from django.urls import path
from da_processor.views import (
    DistributionAuthorizationAPIView,
    HealthCheckView
)

urlpatterns = [
    path('api/health/', HealthCheckView.as_view(), name='health-check'),
    path('api/v1/distribution-authorization/', DistributionAuthorizationAPIView.as_view(), name='da-create')
]