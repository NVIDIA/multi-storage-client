/**
 * File Preview Modal Component
 * Shows file metadata with tabbed interface (extensible for future preview functionality)
 */
import { useState, useEffect } from 'react';
import {
  Modal,
  Spin,
  Alert,
  Descriptions,
  Button,
  Space,
  Typography,
  Tabs,
  message
} from 'antd';
import {
  DownloadOutlined,
  FileTextOutlined,
  CloseOutlined
} from '@ant-design/icons';
import { getFileInfo, downloadFile } from '../services/api';

const { Paragraph } = Typography;

const FilePreview = ({
  visible,
  fileUrl,
  fileName,
  onClose
}) => {
  const [loading, setLoading] = useState(false);
  const [previewData, setPreviewData] = useState(null);
  const [error, setError] = useState(null);
  const [activeTab, setActiveTab] = useState('properties');

  useEffect(() => {
    if (!visible || !fileUrl) {
      // eslint-disable-next-line react-hooks/set-state-in-effect
      setPreviewData(null);
      setError(null);
      setActiveTab('properties');
      return;
    }

    setLoading(true);
    setError(null);

    const fetchFileInfo = async () => {
      try {
        const data = await getFileInfo(fileUrl);
        const fileInfo = {
          name: fileName,
          size: data.content_length || 0,
          last_modified: data.last_modified || '',
          type: data.type || 'file',
          content_type: data.content_type || 'application/octet-stream',
          etag: data.etag || null
        };
        setPreviewData({
          file_info: fileInfo,
          custom_metadata: data.metadata || null
        });
      } catch (err) {
        const errorMsg = err.response?.data?.detail || 'Failed to load file info';
        setError(errorMsg);
      } finally {
        setLoading(false);
      }
    };

    fetchFileInfo();
  }, [visible, fileUrl, fileName]);

  const handleDownload = async () => {
    onClose();

    const hideMessage = message.loading(`Downloading ${fileName}...`, 0);
    try {
      await downloadFile(fileUrl);
      message.success(`${fileName} downloaded successfully`);
    } catch (err) {
      const errorMsg = err.response?.data?.detail || err.message;
      message.error(`Failed to download ${fileName}: ${errorMsg}`);
    } finally {
      hideMessage();
    }
  };

  const formatFileSize = (bytes) => {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i];
  };

  // Format date in custom format: "January 16, 2025, 16:42:56 (UTC-08:00)"
  // Uses local browser timezone
  const formatDate = (isoString) => {
    if (!isoString) return 'N/A';

    const date = new Date(isoString);

    // Format date part: "January 16, 2025"
    const monthNames = ['January', 'February', 'March', 'April', 'May', 'June',
      'July', 'August', 'September', 'October', 'November', 'December'];
    const month = monthNames[date.getMonth()];
    const day = date.getDate();
    const year = date.getFullYear();

    // Format time part: "16:42:56" (in local browser timezone)
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    const seconds = String(date.getSeconds()).padStart(2, '0');

    // Format timezone offset: "(UTC-08:00)" - gets local browser timezone
    const offsetMinutes = -date.getTimezoneOffset();
    const offsetHours = Math.floor(Math.abs(offsetMinutes) / 60);
    const offsetMins = Math.abs(offsetMinutes) % 60;
    const offsetSign = offsetMinutes >= 0 ? '+' : '-';
    const timezone = `UTC${offsetSign}${String(offsetHours).padStart(2, '0')}:${String(offsetMins).padStart(2, '0')}`;

    return `${month} ${day}, ${year}, ${hours}:${minutes}:${seconds} (${timezone})`;
  };

  const renderPropertiesTab = () => {
    if (loading) {
      return (
        <div style={{ textAlign: 'center', padding: '60px 20px' }}>
          <Spin size="large" />
          <p style={{ marginTop: 16, color: '#999' }}>Loading properties...</p>
        </div>
      );
    }

    if (error) {
      return (
        <Alert
          message="Error"
          description={error}
          type="error"
          showIcon
        />
      );
    }

    if (!previewData || !previewData.file_info) {
      return <Alert message="No properties available" type="info" />;
    }

    const { file_info } = previewData;

    return (
      <Descriptions
        bordered
        column={1}
        size="small"
        styles={{ label: { whiteSpace: 'nowrap' } }}
      >
        <Descriptions.Item label="Name">
          {file_info.name}
        </Descriptions.Item>
        <Descriptions.Item label="Size">
          {formatFileSize(file_info.size)}
        </Descriptions.Item>
        <Descriptions.Item label="Type">
          {file_info.type}
        </Descriptions.Item>
        <Descriptions.Item label="Content Type">
          {file_info.content_type || '-'}
        </Descriptions.Item>
        <Descriptions.Item label="Last Modified">
          {formatDate(file_info.last_modified)}
        </Descriptions.Item>
        {file_info.etag && (
          <Descriptions.Item label="ETag">
            {file_info.etag}
          </Descriptions.Item>
        )}
        <Descriptions.Item label="MSC URL">
          <Paragraph
            copyable
            style={{ margin: 0, fontSize: '12px', wordBreak: 'break-all' }}
          >
            {fileUrl}
          </Paragraph>
        </Descriptions.Item>
      </Descriptions>
    );
  };

  const renderCustomMetadataTab = () => {
    if (loading) {
      return (
        <div style={{ textAlign: 'center', padding: '60px 20px' }}>
          <Spin size="large" />
          <p style={{ marginTop: 16, color: '#999' }}>Loading custom metadata...</p>
        </div>
      );
    }

    if (error) {
      return (
        <Alert
          message="Error"
          description={error}
          type="error"
          showIcon
        />
      );
    }

    if (!previewData || !previewData.custom_metadata || Object.keys(previewData.custom_metadata).length === 0) {
      return (
        <Alert
          message="No custom metadata"
          description="This file does not have any custom metadata associated with it."
          type="info"
          showIcon
        />
      );
    }

    const { custom_metadata } = previewData;

    return (
      <Descriptions
        bordered
        column={1}
        size="small"
        styles={{ label: { whiteSpace: 'nowrap' } }}
      >
        {Object.entries(custom_metadata).map(([key, value]) => (
          <Descriptions.Item key={key} label={key}>
            <Paragraph
              copyable
              style={{ margin: 0, fontSize: '12px', wordBreak: 'break-all' }}
            >
              {typeof value === 'object' ? JSON.stringify(value, null, 2) : String(value)}
            </Paragraph>
          </Descriptions.Item>
        ))}
      </Descriptions>
    );
  };

  return (
    <Modal
      title={
        <Space>
          <FileTextOutlined />
          <span>{fileName}</span>
        </Space>
      }
      open={visible}
      onCancel={onClose}
      width={800}
      footer={
        <Space>
          <Button
            icon={<DownloadOutlined />}
            onClick={handleDownload}
          >
            Download
          </Button>
          <Button
            icon={<CloseOutlined />}
            onClick={onClose}
          >
            Close
          </Button>
        </Space>
      }
    >
      <Tabs
        activeKey={activeTab}
        onChange={setActiveTab}
        items={[
          {
            key: 'properties',
            label: 'Properties',
            children: renderPropertiesTab()
          },
          {
            key: 'custom-metadata',
            label: 'Custom Metadata',
            children: renderCustomMetadataTab()
          }
        ]}
      />
    </Modal>
  );
};

export default FilePreview;


