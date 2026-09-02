/**
 * File Manager Component
 * Main file browsing interface using Ant Design components
 */
import { useState, useEffect, useCallback, useRef } from 'react';
import {
  Layout,
  Breadcrumb,
  Table,
  Button,
  Space,
  Select,
  message,
  Alert,
  Tooltip,
  Tag,
  Card,
  Typography,
  Spin
} from 'antd';
import {
  FolderOutlined,
  FileOutlined,
  DownloadOutlined,
  ReloadOutlined,
  HomeOutlined,
  EyeOutlined
} from '@ant-design/icons';
import {
  listFiles,
  downloadFile
} from '../services/api';
import FilePreview from './FilePreview';

const { Content } = Layout;
const { Option } = Select;
const { Text } = Typography;

const FileManager = ({ profiles }) => {
  const [currentPath, setCurrentPath] = useState('');
  const [currentProfile, setCurrentProfile] = useState(profiles[0] || '');
  const [files, setFiles] = useState([]);
  const [loading, setLoading] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState(null);
  const [previewFile, setPreviewFile] = useState(null);
  const [showPreviewModal, setShowPreviewModal] = useState(false);
  const [hasMore, setHasMore] = useState(false);
  const [lastKey, setLastKey] = useState(null);

  // Track current loading request to prevent race conditions when switching profiles
  const loadingRef = useRef({ profile: null, path: null });

  // Ref for the bottom sentinel element (for infinite scroll)
  const bottomSentinelRef = useRef(null);

  // Construct MSC URL from profile and path
  const getMscUrl = useCallback((path = currentPath) => {
    const cleanPath = path.startsWith('/') ? path.slice(1) : path;
    return `msc://${currentProfile}/${cleanPath}`;
  }, [currentProfile, currentPath]);

  // Load files for current directory
  const loadFiles = useCallback(async (append = false, startAfterKey = null) => {
    if (!currentProfile) return;

    // Store what we're loading to detect stale requests
    const loadingProfile = currentProfile;
    const loadingPath = currentPath;
    loadingRef.current = { profile: loadingProfile, path: loadingPath };

    if (append) {
      setLoadingMore(true);
    } else {
      setLoading(true);
      setError(null);
      setHasMore(false);
      setLastKey(null);
      setFiles([]); // Clear files immediately when navigating
    }

    try {
      const cleanPath = currentPath.startsWith('/') ? currentPath.slice(1) : currentPath;
      const url = `msc://${currentProfile}/${cleanPath}`;
      const options = { limit: 1000 };

      // For pagination, use start_after with the last item's key
      if (append && startAfterKey) {
        options.start_after = startAfterKey;
      }

      const response = await listFiles(url, options);

      // Check if this is still the current profile/path before updating state
      if (loadingRef.current.profile !== loadingProfile ||
          loadingRef.current.path !== loadingPath) {
        console.log('Ignoring stale response for', loadingProfile, loadingPath);
        return; // This response is stale, ignore it
      }

      const normalizeName = (raw) => (raw || '').replace(/\/+$/, '').replace(/\/+/g, '/');
      const transformedFiles = response.items.map(item => ({
        key: item.key || item.name,
        name: normalizeName(item.name),
        isDir: item.is_directory || item.type === 'directory',
        size: item.size || 0,
        modDate: item.last_modified || new Date().toISOString(),
        type: item.is_directory || item.type === 'directory' ? 'directory' : 'file'
      }));

      // Update pagination state
      const receivedCount = response.items.length;
      const mightHaveMore = receivedCount === 1000;
      setHasMore(mightHaveMore);

      // Store last key for next page
      if (receivedCount > 0) {
        const lastItem = response.items[receivedCount - 1];
        const key = lastItem.key || lastItem.name;
        setLastKey(key);
      }

      // Append or replace files
      if (append) {
        setFiles(prev => [...prev, ...transformedFiles]);
      } else {
        setFiles(transformedFiles);
      }
    } catch (err) {
      // Check if this is still the current profile/path before showing error
      if (loadingRef.current.profile !== loadingProfile ||
          loadingRef.current.path !== loadingPath) {
        console.log('Ignoring stale error for', loadingProfile, loadingPath);
        return; // This error is stale, ignore it
      }

      const errorMsg = err.response?.data?.detail || 'Failed to load files';
      setError(errorMsg);
      if (!append) {
        setFiles([]);
      }
    } finally {
      // Only clear loading if this is still the current request
      if (loadingRef.current.profile === loadingProfile &&
          loadingRef.current.path === loadingPath) {
        setLoading(false);
        setLoadingMore(false);
      }
    }
  }, [currentProfile, currentPath]);

  // Load files when profile or path changes
  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect
    loadFiles(false);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [currentProfile, currentPath]);

  // Infinite scroll: Observe the bottom sentinel element
  useEffect(() => {
    // Don't set up observer if there's nothing to load
    if (!hasMore || loadingMore) {
      return;
    }

    const currentElement = bottomSentinelRef.current;
    if (!currentElement) {
      return;
    }

    const observer = new IntersectionObserver(
      (entries) => {
        const [entry] = entries;
        // When the sentinel comes into view
        if (entry.isIntersecting) {
          loadFiles(true, lastKey);
        }
      },
      {
        root: null, // viewport
        rootMargin: '100px', // Start loading 100px before reaching the bottom
        threshold: 0.1,
      }
    );

    observer.observe(currentElement);

    return () => {
      observer.unobserve(currentElement);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hasMore, loadingMore, lastKey]);

  // Handle file download
  const handleDownload = async (file) => {
    const hideMessage = message.loading(`Downloading ${file.name}...`, 0);
    try {
      const fileUrl = getMscUrl() + (currentPath ? '/' : '') + file.name;
      await downloadFile(fileUrl);
      message.success(`${file.name} downloaded successfully`);
    } catch (err) {
      const errorMsg = err.response?.data?.detail || err.message;
      message.error(`Failed to download ${file.name}: ${errorMsg}`);
    } finally {
      hideMessage();
    }
  };

  // Handle directory navigation
  const handleOpenFolder = (folder) => {
    const newPath = currentPath ? `${currentPath}/${folder.name}` : folder.name;
    setCurrentPath(newPath);
  };

  // Handle breadcrumb navigation
  const handleNavigate = (index) => {
    if (index === -1) {
      setCurrentPath('');
    } else {
      const parts = currentPath.split('/');
      setCurrentPath(parts.slice(0, index + 1).join('/'));
    }
  };

  // Handle file preview
  const handlePreview = (file) => {
    const fileUrl = getMscUrl() + (currentPath ? '/' : '') + file.name;
    setPreviewFile({ ...file, url: fileUrl });
    setShowPreviewModal(true);
  };

  // Handle preview modal close
  const handlePreviewClose = () => {
    setShowPreviewModal(false);
    setPreviewFile(null);
  };

  // Format file size
  const formatFileSize = (bytes) => {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i];
  };

  // Format date in custom format: "January 16, 2025, 16:42:56 (UTC-08:00)"
  // Uses local browser timezone
  const formatDateTime = (dateString) => {
    const date = new Date(dateString);

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

  // Table columns configuration
  const columns = [
    {
      title: 'Name',
      dataIndex: 'name',
      key: 'name',
      sorter: !hasMore ? (a, b) => {
        const aName = a.name.toLowerCase();
        const bName = b.name.toLowerCase();
        if (a.isDir && !b.isDir) return -1;
        if (!a.isDir && b.isDir) return 1;
        return aName.localeCompare(bName);
      } : false,
      sortDirections: ['ascend', 'descend'],
      showSorterTooltip: !hasMore,
      render: (text, record) => (
        <Space>
          {record.isDir ? (
            <FolderOutlined style={{
              fontSize: 18,
              color: '#faad14'
            }} />
          ) : (
            <FileOutlined style={{ fontSize: 18, color: '#1890ff' }} />
          )}
          <a
            onClick={() => {
              if (record.isDir) {
                handleOpenFolder(record);
              }
            }}
            style={{
              cursor: record.isDir ? 'pointer' : 'default',
              fontWeight: record.isDir ? 600 : 400
            }}
          >
            {record.isDir ? `${text}/` : text}
          </a>
        </Space>
      ),
    },
    {
      title: 'Type',
      dataIndex: 'type',
      key: 'type',
      width: 120,
      filters: [
        { text: 'Folder', value: 'directory' },
        { text: 'File', value: 'file' },
      ],
      onFilter: (value, record) => record.type === value,
      sorter: !hasMore ? (a, b) => a.type.localeCompare(b.type) : false,
      sortDirections: ['ascend', 'descend'],
      showSorterTooltip: !hasMore,
      render: (type) => (
        <Tag color={type === 'directory' ? 'gold' : 'blue'}>
          {type === 'directory' ? 'Folder' : 'File'}
        </Tag>
      ),
    },
    {
      title: 'Size',
      dataIndex: 'size',
      key: 'size',
      width: 120,
      sorter: !hasMore ? (a, b) => {
        if (a.isDir && b.isDir) return 0;
        if (a.isDir) return -1;
        if (b.isDir) return 1;
        return a.size - b.size;
      } : false,
      sortDirections: ['ascend', 'descend'],
      showSorterTooltip: !hasMore,
      render: (size, record) => (
        record.isDir ? '-' : formatFileSize(size)
      ),
    },
    {
      title: 'Last modified',
      dataIndex: 'modDate',
      key: 'modDate',
      width: 300,
      sorter: !hasMore ? (a, b) => {
        if (a.isDir && b.isDir) return 0;
        if (a.isDir) return -1;
        if (b.isDir) return 1;
        return new Date(a.modDate) - new Date(b.modDate);
      } : false,
      sortDirections: ['ascend', 'descend'],
      showSorterTooltip: !hasMore,
      render: (date, record) => (
        record.isDir ? '-' : formatDateTime(date)
      ),
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 120,
      render: (_, record) => (
        <Space size="small">
          {!record.isDir && (
            <>
              <Tooltip title="Preview">
                <Button
                  type="text"
                  icon={<EyeOutlined />}
                  onClick={() => handlePreview(record)}
                  size="small"
                />
              </Tooltip>
              <Tooltip title="Download">
                <Button
                  type="text"
                  icon={<DownloadOutlined />}
                  onClick={() => handleDownload(record)}
                  size="small"
                />
              </Tooltip>
            </>
          )}
        </Space>
      ),
    },
  ];

  // Build breadcrumb items
  const breadcrumbItems = [
    {
      title: (
        <a onClick={() => handleNavigate(-1)}>
          <Space>
            <HomeOutlined />
            <span>{currentProfile}</span>
          </Space>
        </a>
      ),
    },
  ];

  if (currentPath) {
    const parts = currentPath.split('/');
    parts.forEach((part, index) => {
      breadcrumbItems.push({
        title: <a onClick={() => handleNavigate(index)}>{part}</a>,
      });
    });
  }

  return (
    <Layout style={{ minHeight: '100vh', background: '#fff' }}>
      <Content style={{ padding: '24px' }}>
        <Card>
          {/* Toolbar */}
          <Space
            direction="vertical"
            size="middle"
            style={{ width: '100%', marginBottom: 16 }}
          >
            {/* Profile Selector and Actions Bar */}
            <Space wrap style={{ width: '100%', justifyContent: 'space-between' }}>
              <Space>
                <span style={{ fontWeight: 500 }}>Profile:</span>
                <Select
                  value={currentProfile}
                  onChange={(value) => {
                    setCurrentProfile(value);
                    setCurrentPath('');
                  }}
                  style={{ width: 200 }}
                >
                  {profiles.map(profile => (
                    <Option key={profile} value={profile}>{profile}</Option>
                  ))}
                </Select>
              </Space>

              <Space wrap>
                <Button
                  icon={<ReloadOutlined />}
                  onClick={() => loadFiles(false)}
                  disabled={!currentProfile}
                >
                  Refresh
                </Button>
              </Space>
            </Space>

            {/* Breadcrumb */}
            <Breadcrumb items={breadcrumbItems} />

            {/* Error Alert */}
            {error && (
              <Alert
                message="Error"
                description={error}
                type="error"
                closable
                onClose={() => setError(null)}
                showIcon
              />
            )}

            {/* Pagination Info Alert */}
            {hasMore && (
              <Alert
                message="Large listing detected"
                description="Sorting is unavailable until all items have been fully loaded. Scroll to load additional items."
                type="info"
                showIcon
              />
            )}
          </Space>

          {/* File Display */}
          <Table
            columns={columns}
            dataSource={files}
            loading={loading}
            pagination={false}
            locale={{
              emptyText: (
                <div style={{ padding: '40px 0' }}>
                  <FolderOutlined style={{ fontSize: 48, color: '#d9d9d9' }} />
                  <p style={{ marginTop: 16, color: '#999' }}>
                    This directory is empty
                  </p>
                </div>
              ),
            }}
            size="middle"
          />

          {/* Loading indicator for infinite scroll */}
          {loadingMore && (
            <div style={{ textAlign: 'center', padding: '16px' }}>
              <Spin />
            </div>
          )}

          {/* Sentinel element for infinite scroll detection */}
          {hasMore && <div ref={bottomSentinelRef} style={{ height: '1px' }} />}

          {/* Total Item Count */}
          {files.length > 0 && (
            <div style={{ textAlign: 'center', marginTop: 16 }}>
              <Text type="secondary">
                {files.length} item{files.length !== 1 ? 's' : ''}
              </Text>
            </div>
          )}
        </Card>
      </Content>

      {/* File Preview Modal */}
      {previewFile && (
        <FilePreview
          visible={showPreviewModal}
          fileUrl={previewFile.url}
          fileName={previewFile.name}
          onClose={handlePreviewClose}
        />
      )}
    </Layout>
  );
};

export default FileManager;
