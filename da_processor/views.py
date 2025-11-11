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
    """API endpoint for JSON DA submissions"""
    parser_classes = [JSONParser, MultiPartParser, FormParser]

    def post(self, request):
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
    """Health check endpoint"""

    def get(self, request):
        return Response({'status': 'healthy'}, status=status.HTTP_200_OK)