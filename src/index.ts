import { program } from 'commander'
import * as dotenv from 'dotenv'
import { initializeApp, initialize, runCoordinator, runWorker, setupShutdownHandlers, cleanup, getSystemStatus, SyncConfig } from './sync'

// Load environment variables
dotenv.config()

// Store original console methods to avoid recursion
const originalLog = console.log.bind(console)
const originalError = console.error.bind(console)
const originalWarn = console.warn.bind(console)

// Simple console logger with timestamps
const log = {
  info: (message: string, ...args: any[]) => {
    originalLog(`[${new Date().toISOString()}] INFO:`, message, ...args)
  },
  error: (message: string, ...args: any[]) => {
    originalError(`[${new Date().toISOString()}] ERROR:`, message, ...args)
  },
  warn: (message: string, ...args: any[]) => {
    originalWarn(`[${new Date().toISOString()}] WARN:`, message, ...args)
  }
}

// Override console methods to use simple logger
console.log = log.info
console.error = log.error
console.warn = log.warn

program
  .name('mongodb-sync-tool')
  .description('A distributed MongoDB synchronization tool')
  .version('1.0.0')

program
  .option('-m, --mode <mode>', 'Mode: coordinator or worker', 'coordinator')
  .option('-c, --config <path>', 'Configuration file path', './config.json')
  .action(async (options) => {
    try {
      console.log('🚀 Starting MongoDB Sync Tool...')
      console.log('Debug: Starting configuration load...')
      
      // Load configuration
      const config = await loadConfig(options.config)
      console.log('Debug: Configuration loaded successfully')
      
      // Initialize the application
      console.log('Debug: Initializing application...')
      initializeApp(config, options.mode)
      console.log('Debug: App initialized, connecting to databases...')
      await initialize()
      console.log('Debug: Database connections established')
      
      // Setup shutdown handlers
      setupShutdownHandlers()
      
      // Run based on mode
      if (options.mode === 'coordinator') {
        console.log('Starting as COORDINATOR')
        await runCoordinator()
      } else if (options.mode === 'worker') {
        console.log('Starting as WORKER')
        await runWorker()
      } else {
        throw new Error('Invalid mode. Use "coordinator" or "worker"')
      }
      
    } catch (error) {
      console.error('❌ Failed to start MongoDB Sync Tool')
      console.error('Error type:', typeof error)
      console.error('Error object:', error)
      console.error('Error details:', error instanceof Error ? error.message : String(error))
      console.error('Stack trace:', error instanceof Error ? error.stack : 'N/A')
      console.error('Error properties:', error ? Object.keys(error) : 'none')
      if (error && typeof error === 'object') {
        console.error('Error toString:', error.toString())
      }
      await cleanup()
      process.exit(1)
    }
  })

program
  .command('status')
  .description('Get system status')
  .option('-c, --config <path>', 'Configuration file path', './config.json')
  .action(async (options) => {
    try {
      const config = await loadConfig(options.config)
      initializeApp(config, 'worker')
      await initialize()
      
      const status = await getSystemStatus()
      console.log('📊 System Status:')
      console.log(JSON.stringify(status, null, 2))
      
      await cleanup()
      process.exit(0)
    } catch (error) {
      console.error('❌ Failed to get status:')
      console.error('Details:', error instanceof Error ? error.message : error)
      process.exit(1)
    }
  })

program
  .command('validate-config')
  .description('Validate configuration file')
  .option('-c, --config <path>', 'Configuration file path', './config.json')
  .action(async (options) => {
    try {
      const config = await loadConfig(options.config)
      console.log('✅ Configuration is valid')
      console.log('📋 Configuration summary:')
      console.log(`   Collections: ${config.collections.join(', ')}`)
      console.log(`   Source: ${config.sourceDatabase}`)
      console.log(`   Destination: ${config.destDatabase}`)
      console.log(`   Batch Size: ${config.batchSize || 1000}`)
      console.log(`   Max Retries: ${config.maxRetries || 3}`)
      console.log(`   Workers per Collection: ${config.workersPerCollection || 10}`)
    } catch (error) {
      console.error('❌ Configuration validation failed:')
      console.error('Details:', error instanceof Error ? error.message : error)
      process.exit(1)
    }
  })

async function loadConfig(configPath: string): Promise<SyncConfig> {
  try {
    console.log('Debug: Loading config from:', configPath)
    // Try to load from file first
    const fs = await import('fs/promises')
    console.log('Debug: Reading config file...')
    const configFile = await fs.readFile(configPath, 'utf-8')
    console.log('Debug: Config file read, parsing JSON...')
    const config = JSON.parse(configFile)
    
    // Replace environment variables in URI strings
    if (config.sourceUri) {
      config.sourceUri = replaceEnvVars(config.sourceUri)
    }
    if (config.destUri) {
      config.destUri = replaceEnvVars(config.destUri)
    }
    
    return config
  } catch (error) {
    console.log('Debug: Config file loading failed, trying environment variables...')
    console.log('Debug: Config error:', error instanceof Error ? error.message : error)
    // If file doesn't exist, try to load from environment variables
    if (process.env.SOURCE_URI && process.env.DEST_URI && process.env.SOURCE_DATABASE && process.env.DEST_DATABASE) {
      const config: SyncConfig = {
        sourceUri: process.env.SOURCE_URI,
        destUri: process.env.DEST_URI,
        sourceDatabase: process.env.SOURCE_DATABASE,
        destDatabase: process.env.DEST_DATABASE,
        progressDatabase: process.env.PROGRESS_DATABASE,
        batchSize: process.env.BATCH_SIZE ? parseInt(process.env.BATCH_SIZE) : undefined,
        maxRetries: process.env.MAX_RETRIES ? parseInt(process.env.MAX_RETRIES) : undefined,
        retryDelay: process.env.RETRY_DELAY ? parseInt(process.env.RETRY_DELAY) : undefined,
        maxAcceptableLag: process.env.MAX_ACCEPTABLE_LAG ? parseInt(process.env.MAX_ACCEPTABLE_LAG) : undefined,
        workersPerCollection: process.env.WORKERS_PER_COLLECTION ? parseInt(process.env.WORKERS_PER_COLLECTION) : undefined,
        collections: process.env.COLLECTIONS ? process.env.COLLECTIONS.split(',').map(s => s.trim()) : []
      }
      
      console.log('Debug: Using environment variables for config')
      return config
    }
    
    console.log('Debug: Neither config file nor env vars available')
    throw new Error(`Configuration file not found: ${configPath}. Please create a config file or set environment variables.`)
  }
}

function replaceEnvVars(str: string): string {
  return str.replace(/\$\{([^}]+)\}/g, (match, varName) => {
    const value = process.env[varName]
    if (value === undefined) {
      throw new Error(`Environment variable ${varName} is not defined`)
    }
    return value
  })
}

// Handle uncaught exceptions and rejections
process.on('uncaughtException', async (error) => {
  console.error('💥 Uncaught Exception:')
  console.error('Details:', error instanceof Error ? error.message : error)
  console.error('Stack:', error instanceof Error ? error.stack : 'N/A')
  await cleanup()
  process.exit(1)
})

process.on('unhandledRejection', async (reason, promise) => {
  console.error('💥 Unhandled Rejection at:', promise)
  console.error('Reason:', reason instanceof Error ? reason.message : reason)
  console.error('Stack:', reason instanceof Error ? reason.stack : 'N/A')
  await cleanup()
  process.exit(1)
})

// Parse command line arguments
program.parse()

export { loadConfig, replaceEnvVars }