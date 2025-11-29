"""
Email Notification Service for Distribution Authorization (DA) alerts.

This service sends formatted email notifications via AWS SES for missing asset
alerts and other exception notifications in the DA workflow.
"""
import logging
import boto3
from typing import Dict, List
from django.conf import settings

logger = logging.getLogger(__name__)


class EmailNotificationService:
    """
    Service for sending email notifications via AWS Simple Email Service (SES).

    This service:
    - Sends missing asset alert emails with detailed component information
    - Formats notifications in both HTML and plain text
    - Handles recipient list management with fallback to defaults
    - Provides styled email templates for professional notification delivery
    """

    def __init__(self):
        self.ses_client = boto3.client('ses', region_name=settings.AWS_REGION)

    def send_missing_assets_notification(self, missing_assets_info: Dict) -> bool:
        """
        Send email notification about missing required assets for a DA.

        Composes and sends both HTML and plain text versions of the notification
        to configured recipients.

        Args:
            missing_assets_info: Dictionary containing:
                - da_id: Distribution Authorization ID
                - title_name, version_name: Title information
                - licensee_id: Licensee identifier
                - missing_components: List of components with missing assets
                - total_missing_count: Total number of missing assets
                - exception_recipients: Email recipients (comma-separated)

        Returns:
            True if email sent successfully, False otherwise
        """
        try:
            da_id = missing_assets_info.get('da_id', '')
            title_name = missing_assets_info.get('title_name', 'Unknown')
            version_name = missing_assets_info.get('version_name', '')
            licensee_id = missing_assets_info.get('licensee_id', 'Unknown')
            missing_components = missing_assets_info.get('missing_components', [])
            total_missing = missing_assets_info.get('total_missing_count', 0)
            
            recipients = missing_assets_info.get('exception_recipients', '')
            if not recipients:
                recipients = ','.join(settings.DEFAULT_EXCEPTION_RECIPIENTS)
            
            recipient_list = [email.strip() for email in recipients.split(',') if email.strip()]
            
            if not recipient_list:
                logger.warning("[EMAIL] No recipients configured for exception notification")
                return False
            
            subject = f"Missing Assets Alert - DA {da_id} - {title_name}"
            
            body_html = self._build_html_email(
                da_id, title_name, version_name, licensee_id, 
                missing_components, total_missing
            )
            
            body_text = self._build_text_email(
                da_id, title_name, version_name, licensee_id, 
                missing_components, total_missing
            )
            
            response = self.ses_client.send_email(
                Source=settings.SES_FROM_EMAIL,
                Destination={'ToAddresses': recipient_list},
                Message={
                    'Subject': {'Data': subject, 'Charset': 'UTF-8'},
                    'Body': {
                        'Text': {'Data': body_text, 'Charset': 'UTF-8'},
                        'Html': {'Data': body_html, 'Charset': 'UTF-8'}
                    }
                }
            )
            
            logger.info(f"[EMAIL] Sent missing assets notification for DA {da_id} to {len(recipient_list)} recipients")
            logger.info(f"[EMAIL] SES MessageId: {response['MessageId']}")
            
            return True
            
        except Exception as e:
            logger.error(f"[EMAIL] Error sending missing assets notification: {e}", exc_info=True)
            return False

    def _build_html_email(
        self, da_id: str, title_name: str, version_name: str,
        licensee_id: str, missing_components: List[Dict], total_missing: int
    ) -> str:
        """
        Build HTML-formatted email body for missing assets notification.

        Args:
            da_id: Distribution Authorization ID
            title_name: Title name
            version_name: Version name
            licensee_id: Licensee identifier
            missing_components: List of components with missing asset details
            total_missing: Total count of missing assets

        Returns:
            HTML-formatted email string with inline CSS styling
        """
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                .header {{ background-color: #d32f2f; color: white; padding: 20px; text-align: center; }}
                .content {{ padding: 20px; }}
                .info-box {{ background-color: #f5f5f5; border-left: 4px solid #d32f2f; padding: 15px; margin: 20px 0; }}
                .info-box h3 {{ margin-top: 0; color: #d32f2f; }}
                .component {{ margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }}
                .component h4 {{ margin-top: 0; color: #1976d2; }}
                .asset-list {{ list-style-type: none; padding-left: 0; }}
                .asset-list li {{ padding: 8px; margin: 5px 0; background-color: #fff3e0; border-left: 3px solid #ff9800; }}
                .footer {{ margin-top: 30px; padding-top: 20px; border-top: 1px solid #ddd; font-size: 12px; color: #666; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>⚠️ Missing Assets Alert</h1>
            </div>
            
            <div class="content">
                <div class="info-box">
                    <h3>Distribution Authorization Details</h3>
                    <p><strong>DA ID:</strong> {da_id}</p>
                    <p><strong>Title:</strong> {title_name}</p>
                    <p><strong>Version:</strong> {version_name or 'N/A'}</p>
                    <p><strong>Licensee:</strong> {licensee_id}</p>
                    <p><strong>Total Missing Assets:</strong> {total_missing}</p>
                </div>
                
                <h2>Missing Components and Assets</h2>
        """
        
        for component in missing_components:
            component_id = component.get('component_id', 'Unknown')
            missing_assets = component.get('missing_assets', [])
            
            html += f"""
                <div class="component">
                    <h4>Component: {component_id}</h4>
                    <p><strong>Missing Assets Count:</strong> {len(missing_assets)}</p>
                    <ul class="asset-list">
            """
            
            for asset in missing_assets:
                filename = asset.get('filename', 'Unknown')
                full_path = asset.get('full_path', '')
                html += f"<li><strong>{filename}</strong><br><small>{full_path}</small></li>"
            
            html += """
                    </ul>
                </div>
            """
        
        html += """
                <div class="footer">
                    <p>Please take necessary action to ensure these assets are delivered before the due date.</p>
                    <p>This is an automated notification from Route Runner Distribution System.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        return html

    def _build_text_email(
        self, da_id: str, title_name: str, version_name: str,
        licensee_id: str, missing_components: List[Dict], total_missing: int
    ) -> str:
        """
        Build plain text email body for missing assets notification.

        Args:
            da_id: Distribution Authorization ID
            title_name: Title name
            version_name: Version name
            licensee_id: Licensee identifier
            missing_components: List of components with missing asset details
            total_missing: Total count of missing assets

        Returns:
            Plain text formatted email string
        """
        text = f"""
MISSING ASSETS ALERT
{'=' * 60}

Distribution Authorization Details:
- DA ID: {da_id}
- Title: {title_name}
- Version: {version_name or 'N/A'}
- Licensee: {licensee_id}
- Total Missing Assets: {total_missing}

{'=' * 60}

Missing Components and Assets:
"""
        
        for component in missing_components:
            component_id = component.get('component_id', 'Unknown')
            missing_assets = component.get('missing_assets', [])
            
            text += f"\n\nComponent: {component_id}\n"
            text += f"Missing Assets Count: {len(missing_assets)}\n"
            text += "-" * 40 + "\n"
            
            for asset in missing_assets:
                filename = asset.get('filename', 'Unknown')
                full_path = asset.get('full_path', '')
                text += f"  • {filename}\n"
                text += f"    Path: {full_path}\n"
        
        text += f"\n\n{'=' * 60}\n"
        text += "Please take necessary action to ensure these assets are delivered before the due date.\n"
        text += "This is an automated notification from Route Runner Distribution System.\n"
        
        return text