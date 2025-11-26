from rest_framework import serializers

class ComponentSerializer(serializers.Serializer):
    """
    Component information for a Distribution Authorization request.
    
    Each component represents a media asset (e.g., Feature Film, Trailer, Subtitle file)
    that is part of the distribution package.
    """
    component_id = serializers.CharField(
        source='Component ID',
        help_text='Unique identifier for the component (REQUIRED)',
        required=True
    )
    component_name = serializers.CharField(
        source='Component Name',
        required=False,
        allow_blank=True,
        help_text='Human-readable name of the component (OPTIONAL)'
    )
    component_type = serializers.CharField(
        source='Component Type',
        required=False,
        allow_blank=True,
        help_text='Type of component: Feature, Trailer, Subtitle, Audio, etc. (OPTIONAL)'
    )
    required_flag = serializers.CharField(
        source='Required Flag',
        required=False,
        default='FALSE',
        help_text='Whether this component is required for delivery: TRUE or FALSE (OPTIONAL, default: FALSE)'
    )
    watermark_required = serializers.CharField(
        source='Watermark Required',
        required=False,
        default='FALSE',
        help_text='Whether watermarking is required: TRUE or FALSE (OPTIONAL, default: FALSE)'
    )


class MainBodyAttributesSerializer(serializers.Serializer):
    """
    Main body attributes for Distribution Authorization request.
    
    Contains core metadata about the title, version, licensee, and licensing terms.
    """
    
    # REQUIRED FIELDS
    licensee_id = serializers.CharField(
        source='Licensee ID',
        help_text='Unique identifier for the licensee/distributor (REQUIRED). Example: PRIME_VIDEO, NETFLIX, HULU',
        required=True
    )
    title_id = serializers.CharField(
        source='Title ID',
        help_text='Unique identifier for the title/content (REQUIRED). Example: TITLE_001',
        required=True
    )
    version_id = serializers.CharField(
        source='Version ID',
        help_text='Version identifier for this specific cut/version (REQUIRED). Example: V1, THEATRICAL, DIRECTORS_CUT',
        required=True
    )
    release_year = serializers.CharField(
        source='Release Year',
        help_text='Year of original release (REQUIRED). Format: YYYY. Example: 2024',
        required=True
    )
    license_period_start = serializers.CharField(
        source='License Period Start',
        help_text='Start date of license period (REQUIRED). Format: YYYY-MM-DD or ISO 8601. Example: 2024-01-01',
        required=True
    )
    license_period_end = serializers.CharField(
        source='License Period End',
        help_text='End date of license period (REQUIRED). Format: YYYY-MM-DD or ISO 8601. Example: 2024-12-31',
        required=True
    )
    
    # OPTIONAL FIELDS
    title_name = serializers.CharField(
        source='Title Name',
        required=False,
        allow_blank=True,
        help_text='Human-readable title name (OPTIONAL). Example: The Great Adventure'
    )
    title_eidr_id = serializers.CharField(
        source='Title EIDR ID',
        required=False,
        allow_blank=True,
        help_text='EIDR (Entertainment Identifier Registry) ID for the title (OPTIONAL). Example: 10.5240/XXXX-XXXX-XXXX-XXXX-XXXX-X'
    )
    version_name = serializers.CharField(
        source='Version Name',
        required=False,
        allow_blank=True,
        help_text='Human-readable version name (OPTIONAL). Example: Theatrical Cut'
    )
    version_eidr_id = serializers.CharField(
        source='Version EIDR ID',
        required=False,
        allow_blank=True,
        help_text='EIDR ID for this specific version (OPTIONAL)'
    )
    da_description = serializers.CharField(
        source='DA Description',
        required=False,
        allow_blank=True,
        help_text='Description or notes about this DA (OPTIONAL). Example: Q1 2024 Release Package'
    )
    due_date = serializers.CharField(
        source='Due Date',
        required=False,
        allow_blank=True,
        help_text='Due date for delivery (OPTIONAL). Format: YYYY-MM-DD or ISO 8601'
    )
    earliest_delivery_date = serializers.CharField(
        source='Earliest Delivery Date',
        required=False,
        allow_blank=True,
        help_text='Earliest date for delivery/manifest generation (OPTIONAL). Format: YYYY-MM-DD or ISO 8601'
    )
    territories = serializers.CharField(
        source='Territories',
        required=False,
        allow_blank=True,
        help_text='Licensed territories (OPTIONAL). Example: US, CA, MX or Worldwide'
    )
    exception_notification_date = serializers.CharField(
        source='Exception Notification Date',
        required=False,
        allow_blank=True,
        help_text='Date to send exception notifications if delivery incomplete (OPTIONAL). Format: YYYY-MM-DD or ISO 8601'
    )
    exception_recipients = serializers.CharField(
        source='Exception Recipients',
        required=False,
        allow_blank=True,
        help_text='Comma-separated email addresses for exception notifications (OPTIONAL). Example: user1@example.com,user2@example.com'
    )
    internal_studio_id = serializers.CharField(
        source='Internal Studio ID',
        required=False,
        allow_blank=True,
        help_text='Internal studio identifier (OPTIONAL). Example: STUDIO_001'
    )
    studio_system_id = serializers.CharField(
        source='Studio System ID',
        required=False,
        allow_blank=True,
        help_text='Studio system identifier (OPTIONAL)'
    )


class DARequestSerializer(serializers.Serializer):
    """
    Complete Distribution Authorization request payload.
    
    This is the top-level structure for JSON-based DA submissions.
    """
    main_body_attributes = MainBodyAttributesSerializer(
        help_text='Main metadata and licensing information for the DA'
    )
    components = ComponentSerializer(
        many=True,
        help_text='List of components (media assets) included in this DA. At least one component is required.'
    )


class DAResponseSerializer(serializers.Serializer):
    """
    Successful Distribution Authorization submission response.
    
    Returned when a DA is successfully created and stored.
    """
    success = serializers.BooleanField(
        help_text='Indicates successful processing (always true for 201 responses)'
    )
    id = serializers.CharField(
        help_text='Generated unique DA ID. Use this ID to track the DA through the system.'
    )
    title_id = serializers.CharField(
        help_text='Echo of the submitted Title ID'
    )
    version_id = serializers.CharField(
        help_text='Echo of the submitted Version ID'
    )
    licensee_id = serializers.CharField(
        help_text='Echo of the submitted Licensee ID'
    )
    components_count = serializers.IntegerField(
        help_text='Number of components successfully processed'
    )


class ErrorResponseSerializer(serializers.Serializer):
    """
    Error response structure.
    
    Returned for validation errors (400) and server errors (500).
    """
    error = serializers.CharField(
        help_text='Human-readable error message describing what went wrong'
    )


class HealthCheckResponseSerializer(serializers.Serializer):
    """
    Health check response.
    
    Simple status indicator for monitoring and load balancers.
    """
    status = serializers.CharField(
        help_text='Health status of the API service. Returns "healthy" when operational.'
    )