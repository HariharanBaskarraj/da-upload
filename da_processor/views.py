"""
Django REST Framework views for Distribution Authorization API endpoints.

This module provides API views for DA creation via JSON and CSV uploads,
along with health check endpoints for service monitoring.
"""
import logging
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from da_processor.processors.json_processor import JSONProcessor
from rest_framework.parsers import MultiPartParser, FormParser, JSONParser
from da_processor.processors.json_processor import JSONProcessor
from da_processor.processors.csv_processor import CSVProcessor

logger = logging.getLogger(__name__)


class DistributionAuthorizationAPIView(APIView):
    """
    API endpoint for Distribution Authorization creation.

    Supports both JSON and CSV file uploads for DA creation.
    Automatically routes requests to appropriate processor based on content type.

    Supported content types:
        - application/json: JSON payload processing
        - multipart/form-data: CSV file upload
        - text/csv: CSV file upload
    """
    parser_classes = [JSONParser, MultiPartParser, FormParser]

    def post(self, request):
        """
        Create a Distribution Authorization from JSON or CSV.

        Args:
            request: Django REST Framework request object

        Returns:
            Response with DA creation result

        Status Codes:
            - 201: DA created successfully
            - 400: Invalid request or validation error
            - 500: Internal server error
        """
        try:
            content_type = request.content_type

            if 'application/json' in content_type:
                payload = request.data
                processor = JSONProcessor()
                result = processor.process(payload)
            elif 'multipart/form-data' in content_type or 'text/csv' in content_type:
                if 'file' not in request.FILES:
                    return Response(
                        {'error': 'No file provided in request'},
                        status=status.HTTP_400_BAD_REQUEST
                    )
                
                csv_file = request.FILES['file']
                csv_content = csv_file.read().decode('utf-8')
                
                processor = CSVProcessor()
                result = processor.process(csv_content)
            else:
                return Response(
                    {'error': f'Unsupported content type: {content_type}'},
                    status=status.HTTP_400_BAD_REQUEST
                )

            return Response(result, status=status.HTTP_201_CREATED)

        except ValueError as e:
            logger.error(f"Validation error: {str(e)}")
            return Response(
                {'error': str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )
        except Exception as e:
            logger.error(f"Error processing DA upload: {str(e)}", exc_info=True)
            return Response(
                {'error': 'Internal server error processing DA upload'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class HealthCheckView(APIView):
    """
    Health check endpoint for service monitoring.

    Returns a simple healthy status response for load balancer
    and monitoring tools.
    """
    logger.disabled = True

    def get(self, request):
        """
        Health check endpoint.

        Returns:
            Response with healthy status
        """
        return Response({'status': 'healthy'}, status=status.HTTP_200_OK)