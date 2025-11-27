import logging
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from drf_spectacular.utils import extend_schema, OpenApiExample, OpenApiParameter
from drf_spectacular.types import OpenApiTypes
from da_processor.processors.json_processor import JSONProcessor
from rest_framework.parsers import MultiPartParser, FormParser, JSONParser
from da_processor.processors.json_processor import JSONProcessor
from da_processor.processors.csv_processor import CSVProcessor
from da_processor.serializers import DARequestSerializer, DAResponseSerializer, ErrorResponseSerializer, HealthCheckResponseSerializer
from da_processor.exceptions import ValidationError

logger = logging.getLogger(__name__)


class DistributionAuthorizationAPIView(APIView):
    """API endpoint for JSON DA submissions"""
    parser_classes = [JSONParser, MultiPartParser, FormParser]

    @extend_schema(
        tags=['Distribution Authorization'],
        summary='Submit Distribution Authorization',
        description='''
        Submit a Distribution Authorization (DA) request for media content delivery.
        
        ## Supported Formats
        
        ### JSON Format (application/json)
        Submit structured JSON with `main_body_attributes` and `components` arrays.
        
        ### CSV Format (multipart/form-data)
        Upload a CSV file with the following structure:
        - Main body attributes in rows (Field Name, Value)
        - Component section divider row: "Component ID", "Required Flag", "Watermark Required"
        - Component rows following the divider
        
        ## Processing Flow
        1. Request is validated for required fields
        2. DA record is created in DynamoDB
        3. Components are stored and linked to the DA
        4. If Earliest Delivery Date is provided, a manifest generation schedule is created
        5. If Exception Notification Date is provided, an exception notification schedule is created
        6. Success response with generated DA ID is returned
        
        ## Required Fields
        - Licensee ID
        - Title ID
        - Version ID
        - Release Year
        - License Period Start
        - License Period End
        - At least one Component with Component ID
        
        ## Date Formats
        All dates accept:
        - ISO 8601: `2024-01-01T00:00:00Z`
        - Simple date: `2024-01-01`
        - Date with time: `2024-01-01 14:30:00`
        
        ## Error Handling
        - **400 Bad Request**: Validation errors, missing required fields, invalid format
        - **500 Internal Server Error**: Database errors, AWS service errors
        ''',
        request={
            'application/json': DARequestSerializer,
            'multipart/form-data': {
                'type': 'object',
                'properties': {
                    'file': {
                        'type': 'string',
                        'format': 'binary',
                        'description': 'CSV file containing DA information'
                    }
                }
            }
        },
        responses={
            201: DAResponseSerializer,
            400: ErrorResponseSerializer,
            500: ErrorResponseSerializer,
        },
        examples=[
            OpenApiExample(
                'Minimal JSON Request (Required Fields Only)',
                value={
                    "main_body_attributes": {
                        "Licensee ID": "PRIME_VIDEO",
                        "Title ID": "TITLE_001",
                        "Version ID": "V1",
                        "Release Year": "2024",
                        "License Period Start": "2024-01-01",
                        "License Period End": "2024-12-31"
                    },
                    "components": [
                        {
                            "Component ID": "COMP_001"
                        }
                    ]
                },
                request_only=True,
            ),
            OpenApiExample(
                'Complete JSON Request (All Fields)',
                value={
                    "main_body_attributes": {
                        "Licensee ID": "PRIME_VIDEO",
                        "Title ID": "TITLE_001",
                        "Version ID": "THEATRICAL",
                        "Release Year": "2024",
                        "License Period Start": "2024-01-01T00:00:00Z",
                        "License Period End": "2024-12-31T23:59:59Z",
                        "Title Name": "The Great Adventure",
                        "Title EIDR ID": "10.5240/XXXX-XXXX-XXXX-XXXX-XXXX-X",
                        "Version Name": "Theatrical Cut",
                        "Version EIDR ID": "10.5240/YYYY-YYYY-YYYY-YYYY-YYYY-Y",
                        "DA Description": "Q1 2024 Release - North America",
                        "Due Date": "2024-01-15",
                        "Earliest Delivery Date": "2024-01-10",
                        "Territories": "US, CA, MX",
                        "Exception Notification Date": "2024-01-20",
                        "Exception Recipients": "ops@studio.com",
                        "Internal Studio ID": "STUDIO_001",
                        "Studio System ID": "SYS_ABC123"
                    },
                    "components": [
                        {
                            "Component ID": "FEATURE_4K_HDR",
                            "Component Name": "Feature Film 4K HDR",
                            "Component Type": "Feature",
                            "Required Flag": "TRUE",
                            "Watermark Required": "FALSE"
                        },
                        {
                            "Component ID": "TRAILER_HD",
                            "Component Name": "Theatrical Trailer HD",
                            "Component Type": "Trailer",
                            "Required Flag": "FALSE",
                            "Watermark Required": "TRUE"
                        },
                        {
                            "Component ID": "SUBTITLES_EN",
                            "Component Name": "English Subtitles",
                            "Component Type": "Subtitle",
                            "Required Flag": "TRUE",
                            "Watermark Required": "FALSE"
                        }
                    ]
                },
                request_only=True,
            ),
            OpenApiExample(
                'Success Response',
                value={
                    "success": True,
                    "id": "01JDXYZ123ABC456DEF789",
                    "title_id": "TITLE_001",
                    "version_id": "THEATRICAL",
                    "licensee_id": "PRIME_VIDEO",
                    "components_count": 3
                },
                response_only=True,
                status_codes=['201'],
            ),
            OpenApiExample(
                'Validation Error - Missing Required Fields',
                value={
                    "error": "Missing required fields: Title ID, Version ID"
                },
                response_only=True,
                status_codes=['400'],
            ),
            OpenApiExample(
                'Validation Error - No Components',
                value={
                    "error": "No components found in payload"
                },
                response_only=True,
                status_codes=['400'],
            ),
            OpenApiExample(
                'Validation Error - Invalid Date Format',
                value={
                    "error": "Invalid License Period Start date: not-a-date"
                },
                response_only=True,
                status_codes=['400'],
            ),
            OpenApiExample(
                'Server Error',
                value={
                    "error": "Internal server error processing DA upload"
                },
                response_only=True,
                status_codes=['500'],
            ),
        ]
    )

    def post(self, request):
        logger.info(
            "DA submission received",
            extra={
                'event_type': 'REQUEST',
                'content_type': request.content_type,
                'method': request.method
            }
        )

        try:
            content_type = request.content_type

            if 'application/json' in content_type:
                payload = request.data
                processor = JSONProcessor()
                result = processor.process(payload)
            elif 'multipart/form-data' in content_type or 'text/csv' in content_type:
                if 'file' not in request.FILES:
                    raise ValidationError('No file provided in request')
                
                csv_file = request.FILES['file']
                csv_content = csv_file.read().decode('utf-8')
                
                processor = CSVProcessor()
                result = processor.process(csv_content)
            else:
                raise ValidationError(f'Unsupported content type: {content_type}')
            
            logger.info(
                "DA submission successful",
                extra={
                    'event_type': 'RESPONSE',
                    'da_id': result.get('id'),
                    'title_id': result.get('title_id'),
                    'status_code': 201
                }
            )

            return Response(result, status=status.HTTP_201_CREATED)

        except ValueError as e:
            logger.error(
                f"Validation error: {str(e)}",
                extra={'event_type': 'ERROR', 'error_type': 'ValidationError'}
            )
            return Response(
                {'error': str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )
        except Exception as e:
            logger.error(
                f"Error processing DA: {str(e)}",
                extra={'event_type': 'ERROR', 'error_type': type(e).__name__},
                exc_info=True
            )
            return Response(
                {'error': 'Internal server error processing DA upload'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class HealthCheckView(APIView):
    """Health check endpoint"""

    logger.disabled = True

    @extend_schema(
        tags=['Health'],
        summary='Health Check',
        description='Check if the API service is running and healthy',
        responses={
            200: HealthCheckResponseSerializer,
        },
        examples=[
            OpenApiExample(
                'Healthy Response',
                value={'status': 'healthy'},
                response_only=True,
            ),
        ]
    )

    def get(self, request):
        return Response({'status': 'healthy'}, status=status.HTTP_200_OK)