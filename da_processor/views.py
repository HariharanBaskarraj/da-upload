import logging
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from config import settings
from da_processor.processors.json_processor import JSONProcessor
from da_processor.services.default_values_service import DefaultValuesService

logger = logging.getLogger(__name__)


class DistributionAuthorizationAPIView(APIView):
    """API endpoint for JSON DA submissions"""

    def post(self, request):
        """Process DA from JSON payload"""
        try:
            payload = request.data
            
            if not payload:
                return Response(
                    {'error': 'Empty payload'},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Process the DA
            processor = JSONProcessor()
            result = processor.process(payload)
            
            logger.info(f"DA processed successfully: {result}")
            
            return Response(result, status=status.HTTP_201_CREATED)
            
        except ValueError as e:
            logger.error(f"Validation error: {str(e)}")
            return Response(
                {'error': str(e), 'type': 'validation_error'},
                status=status.HTTP_400_BAD_REQUEST
            )
        except Exception as e:
            logger.error(f"Processing error: {str(e)}")
            return Response(
                {'error': 'Internal processing error', 'details': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class LicenseeDefaultsAPIView(APIView):
    """API endpoint for viewing current default configuration"""

    def get(self, request, licensee_id):
        """Get default values configuration"""
        try:
            defaults = {
                'licensee_id': licensee_id,
                'default_delivery_window_days': settings.DEFAULT_DELIVERY_WINDOW_DAYS,
                'exception_notification_days': settings.EXCEPTION_NOTIFICATION_DAYS,
                'default_exception_recipients': settings.DEFAULT_EXCEPTION_RECIPIENTS,
                'note': 'These are system-level defaults configured via environment variables'
            }
            
            return Response(defaults, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Error retrieving defaults: {str(e)}")
            return Response(
                {'error': 'Failed to retrieve defaults'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class HealthCheckView(APIView):
    """Health check endpoint"""

    def get(self, request):
        return Response({'status': 'healthy'}, status=status.HTTP_200_OK)