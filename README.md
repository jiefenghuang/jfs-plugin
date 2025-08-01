This is an implementation of the JuiceFS object storage plugin, which communicates with JuiceFS over TCP or UDS.

## Usage
1. Implement interface plugin in https://github.com/jiefenghuang/jfs-plugin/blob/main/pkg/server/plugin.go. (You can refer to the approach in plugin_test.go.)
2. Build and run with url "unix://xxx" or "tcp://localhost:xxx"
3. Format and mount JuiceFS with JFS_PLUGIN_URL env and storage type "plugin".
