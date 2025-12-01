import { MongoClient, Db, Collection, ChangeStream, ObjectId } from 'mongodb'
import { EventEmitter } from 'events'
import * as os from 'os'
import * as crypto from 'crypto'

// Configuration interface for the sync tool
export interface SyncConfig {
    sourceUri: string
    destUri: string
    sourceDatabase: string
    destDatabase: string
    progressDatabase?: string
    batchSize?: number
    maxRetries?: number
    retryDelay?: number
    maxAcceptableLag?: number
    workersPerCollection?: number
    collections: string[]
}

interface LagMetrics {
    lastChangeTime: number
    lastProcessedTime: number
    currentLag: number
    averageLag: number
    maxLag: number
    minLag: number
    processingTime: number
    changeCount: number
    errorCount: number
    lagHistory: LagHistoryEntry[]
    lastLogTime: number
}

interface LagHistoryEntry {
    timestamp: number
    lag: number
    processingTime: number
    isError: boolean
}

interface Task {
    taskId: string
    type: 'migration' | 'realtime_sync' | 'catchup'
    collection: string
    taskIndex?: number
    totalTasks?: number
    lastId?: any
    batchSize?: number
    query?: any
    status: 'pending' | 'assigned' | 'running' | 'completed' | 'failed'
    priority: number
    migrationStartTime?: Date
    assignedTo: string | null
    attempts: number
    maxAttempts?: number
    persistent?: boolean
    assignedAt?: Date
    startedAt?: Date
    completedAt?: Date
    failedAt?: Date
    error?: string
    retryCount?: number
    metadata?: any
    lastHeartbeat?: Date
    workerHeartbeat?: Date
    lastUpdated?: Date
    reassignedAt?: Date
    reassignReason?: string
}

interface WorkerDocument {
    workerId: string
    mode: 'coordinator' | 'worker'
    hostname: string
    pid: number
    status: 'active' | 'idle' | 'dead' | 'offline'
    registeredAt: Date
    lastHeartbeat: Date
    currentTask: string | null
    completedTasks: number
    failedTasks: number
    diedAt?: Date
    shutdownAt?: Date
}

interface ProgressDocument {
    collection: string
    totalCount: number
    migratedCount: number
    status: 'migration_pending' | 'migration_in_progress' | 'migration_complete' | 'ready_for_realtime' | 'active_sync'
    totalTasks: number
    completedTasks: number
    migrationStartTime: Date
    migrationCompletedAt?: Date
    catchUpCompletedAt?: Date
    realtimeSyncStartedAt?: Date
    realtimeSyncWorker?: string | null
    realtimeSyncWorkers?: string[]
    indexesCompletedAt?: Date
    createdAt: Date
    lastUpdated: Date
    resumeToken?: any
    note?: string
}

interface AppState {
    config: SyncConfig | null
    mode: 'coordinator' | 'worker'
    workerId: string | null
    sourceClient: MongoClient | null
    destClient: MongoClient | null
    sourceDb: Db | null
    destDb: Db | null
    progressDb: Db | null
    isRunning: boolean
    changeStreams: Map<string, ChangeStream>
    batchSize: number
    maxRetries: number
    retryDelay: number
    collectionsToSync: string[]
    
    tasksCollection: Collection<Task> | null
    workersCollection: Collection<WorkerDocument> | null
    progressCollection: Collection<ProgressDocument> | null
    
    taskPollingInterval: NodeJS.Timeout | null
    heartbeatInterval: NodeJS.Timeout | null
    
    emitter: EventEmitter
    
    lagMetrics: Map<string, LagMetrics>
    lagMonitorInterval: NodeJS.Timeout | null
    maxAcceptableLag: number
    workersPerCollection: number
}

const appState: AppState = {
    config: null,
    mode: 'coordinator',
    workerId: null,
    sourceClient: null,
    destClient: null,
    sourceDb: null,
    destDb: null,
    progressDb: null,
    isRunning: false,
    changeStreams: new Map<string, ChangeStream>(),
    batchSize: 1000,
    maxRetries: 3,
    retryDelay: 5000,
    collectionsToSync: [],
    
    tasksCollection: null,
    workersCollection: null,
    progressCollection: null,
    
    taskPollingInterval: null,
    heartbeatInterval: null,
    
    emitter: new EventEmitter(),
    
    lagMetrics: new Map<string, LagMetrics>(),
    lagMonitorInterval: null,
    maxAcceptableLag: 10000,
    workersPerCollection: 10
}

export function initializeApp(config: SyncConfig, mode: 'coordinator' | 'worker' = 'coordinator'): AppState {
    appState.config = config
    appState.mode = mode
    appState.workerId = `${mode}_${os.hostname()}_${crypto.randomBytes(4).toString('hex')}`
    appState.batchSize = config.batchSize || 1000
    appState.maxRetries = config.maxRetries || 3
    appState.retryDelay = config.retryDelay || 5000
    appState.collectionsToSync = config.collections || []
    appState.maxAcceptableLag = config.maxAcceptableLag || 10000
    appState.workersPerCollection = config.workersPerCollection || 10

    validateConfig()
    return appState
}

function validateConfig(): void {
    if (!appState.config?.sourceUri || !appState.config?.destUri) {
        throw new Error('Source and destination URIs are required')
    }
    
    if (!appState.config?.sourceDatabase || !appState.config?.destDatabase) {
        throw new Error('Source and destination databases are required')
    }
    
    if (!appState.collectionsToSync.length) {
        throw new Error('At least one collection must be specified')
    }
    
    console.log(`>> ${appState.mode.toUpperCase()} Mode - Worker ID: ${appState.workerId}`)
    console.log(`Collections configured for sync: ${appState.collectionsToSync.join(', ')}`)
    console.log(`Workflow: Migration -> Catch-up -> Real-time Sync -> Index Creation`)
}

export async function initialize(): Promise<void> {
    try {
        await connectToDatabases()
        await setupTaskManagement()
        await registerWorker()
        
        console.log('MongoDB Sync Tool initialized successfully')
    } catch (error) {
        console.error('Failed to initialize:')
        console.error('Error details:', error instanceof Error ? error.message : error)
        console.error('Stack trace:', error instanceof Error ? error.stack : 'N/A')
        throw error
    }
}

async function connectToDatabases(): Promise<void> {
    console.log('Connecting to databases...')
    
    if (!appState.config) throw new Error('Configuration not initialized')
    
    appState.sourceClient = new MongoClient(appState.config.sourceUri, {
        maxPoolSize: 10,
        serverSelectionTimeoutMS: 5000,
    })
    await appState.sourceClient.connect()
    appState.sourceDb = appState.sourceClient.db(appState.config.sourceDatabase)
    
    appState.destClient = new MongoClient(appState.config.destUri, {
        maxPoolSize: 10,
        serverSelectionTimeoutMS: 5000,
    })
    await appState.destClient.connect()
    appState.destDb = appState.destClient.db(appState.config.destDatabase)
    
    appState.progressDb = appState.destClient.db(appState.config.progressDatabase || 'sync_progress')
    
    console.log('Database connections established')
}

async function setupTaskManagement(): Promise<void> {
    console.log('Setting up task management...')
    
    if (!appState.progressDb) throw new Error('Progress database not connected')
    
    appState.tasksCollection = appState.progressDb.collection<Task>('sync_tasks')
    appState.workersCollection = appState.progressDb.collection<WorkerDocument>('sync_workers')
    appState.progressCollection = appState.progressDb.collection<ProgressDocument>('sync_progress')
    
    // Create indexes for efficient task management
    await appState.tasksCollection.createIndex({ status: 1, priority: -1, _id: 1 })
    await appState.tasksCollection.createIndex({ collection: 1 })
    await appState.tasksCollection.createIndex({ assignedTo: 1 })
    
    await appState.workersCollection.createIndex({ workerId: 1 }, { unique: true })
    await appState.workersCollection.createIndex({ lastHeartbeat: 1 })
    await appState.workersCollection.createIndex({ mode: 1, status: 1 })
    
    await appState.progressCollection.createIndex({ collection: 1 }, { unique: true })
    await appState.progressCollection.createIndex({ status: 1 })
    
    console.log('Task management setup complete')
}

async function registerWorker(): Promise<void> {
    if (!appState.workersCollection || !appState.workerId) {
        throw new Error('Worker collection or ID not initialized')
    }
    
    const workerDoc: WorkerDocument = {
        workerId: appState.workerId,
        mode: appState.mode,
        hostname: os.hostname(),
        pid: process.pid,
        status: 'active',
        registeredAt: new Date(),
        lastHeartbeat: new Date(),
        currentTask: null,
        completedTasks: 0,
        failedTasks: 0
    }
    
    await appState.workersCollection.replaceOne(
        { workerId: appState.workerId },
        workerDoc,
        { upsert: true }
    )
    
    // Start heartbeat for workers
    appState.heartbeatInterval = setInterval(async () => {
        await sendHeartbeat()
    }, 30000)
    
    console.log(`${appState.mode.toUpperCase()} registered: ${appState.workerId}`)
}

async function sendHeartbeat(): Promise<void> {
    try {
        if (!appState.workersCollection || !appState.workerId) return
        
        await appState.workersCollection.updateOne(
            { workerId: appState.workerId },
            {
                $set: {
                    lastHeartbeat: new Date(),
                    status: 'active'
                }
            }
        )
    } catch (error) {
        console.error('Error sending heartbeat:')
        console.error('Details:', error instanceof Error ? error.message : error)
    }
}

export async function runCoordinator(): Promise<void> {
    if (appState.mode !== 'coordinator') {
        throw new Error('Not in coordinator mode')
    }
    
    console.log('>> Starting COORDINATOR workflow...')
    appState.isRunning = true
    
    try {
        await detectCoordinatorRestart()
        await generateMigrationTasks()
        await waitForMigrationAndCatchUp()
        await createIndexes()
        await signalRealTimeSync()
        await monitorSystem()
    } catch (error) {
        console.error('Coordinator error:')
        console.error('Details:', error instanceof Error ? error.message : error)
        console.error('Stack:', error instanceof Error ? error.stack : 'N/A')
        throw error
    }
}

async function detectCoordinatorRestart(): Promise<boolean> {
    if (!appState.tasksCollection || !appState.progressCollection) {
        return false
    }
    
    try {
        const hasTasks = await appState.tasksCollection.countDocuments({}) > 0
        const hasProgress = await appState.progressCollection.countDocuments({}) > 0
        
        if (hasTasks || hasProgress) {
            console.log('[RESTART] Detected previous coordinator session - handling restart')
            await handleCoordinatorRestart()
            return true
        }
        
        return false
    } catch (error) {
        console.error('Error detecting coordinator restart:')
        console.error('Details:', error instanceof Error ? error.message : error)
        return false
    }
}

async function handleCoordinatorRestart(): Promise<void> {
    if (!appState.tasksCollection || !appState.progressCollection) {
        return
    }
    
    try {
        console.log('[RESTART] Cleaning up from previous coordinator session...')
        
        // Reset running tasks to pending
        await appState.tasksCollection.updateMany(
            { status: 'running' },
            { 
                $set: { 
                    status: 'pending',
                    assignedTo: null,
                    startedAt: undefined
                } 
            }
        )
        
        // Reset assigned tasks to pending
        await appState.tasksCollection.updateMany(
            { status: 'assigned' },
            { 
                $set: { 
                    status: 'pending',
                    assignedTo: null,
                    assignedAt: undefined
                } 
            }
        )
        
        console.log('[RESTART] Reset running/assigned tasks to pending state')
        
        // Check if we need to generate additional catch-up tasks
        await generateCatchupTasksForRestart()
        
        console.log('[RESTART] Coordinator restart handling complete')
        
    } catch (error) {
        console.error('Error handling coordinator restart:', error)
        throw error
    }
}

async function generateCatchupTasksForRestart(): Promise<void> {
    if (!appState.progressCollection || !appState.tasksCollection) {
        return
    }
    
    try {
        // Find collections that were in progress during restart
        const progressDocs = await appState.progressCollection.find({
            status: { $in: ['migration_in_progress', 'migration_complete'] }
        }).toArray()
        
        for (const progress of progressDocs) {
            // Generate catch-up tasks for data that may have been missed during restart
            const existingCatchupTasks = await appState.tasksCollection.countDocuments({
                collection: progress.collection,
                type: 'catchup'
            })
            
            if (existingCatchupTasks === 0 && progress.migrationStartTime) {
                await appState.tasksCollection.insertOne({
                    taskId: `catchup_${progress.collection}_${crypto.randomBytes(4).toString('hex')}`,
                    type: 'catchup',
                    collection: progress.collection,
                    query: { _id: { $gte: progress.migrationStartTime } },
                    status: 'pending',
                    priority: 80,
                    assignedTo: null,
                    attempts: 0
                })
                
                console.log(`[RESTART] Generated catch-up task for ${progress.collection}`)
            }
        }
        
    } catch (error) {
        console.error('Error generating catch-up tasks for restart:', error)
        throw error
    }
}

async function generateMigrationTasks(): Promise<void> {
    console.log('[TASKS] Generating migration tasks for workers...')
    
    if (!appState.sourceDb || !appState.destDb || !appState.tasksCollection || !appState.progressCollection) {
        throw new Error('Required database connections not established')
    }
    
    const collections = await getCollectionsToSync()
    const migrationStartTime = new Date()
    
    for (const collectionName of collections) {
        console.log(`[TASKS] Processing collection: ${collectionName}`)
        
        try {
            // Get source collection stats
            const sourceCollection = appState.sourceDb.collection(collectionName)
            const totalCount = await sourceCollection.estimatedDocumentCount()
            
            console.log(`[TASKS] ${collectionName}: ${totalCount} total documents`)
            
            // Calculate number of tasks needed
            const documentsPerTask = 2000
            const totalTasks = Math.max(1, Math.ceil(totalCount / documentsPerTask))
            
            console.log(`[TASKS] Creating ${totalTasks} migration tasks for ${collectionName}`)
            
            // Initialize progress document
            await appState.progressCollection.replaceOne(
                { collection: collectionName },
                {
                    collection: collectionName,
                    totalCount: totalCount,
                    migratedCount: 0,
                    status: 'migration_pending',
                    totalTasks: totalTasks,
                    completedTasks: 0,
                    migrationStartTime: migrationStartTime,
                    createdAt: new Date(),
                    lastUpdated: new Date()
                },
                { upsert: true }
            )
            
            // Create migration tasks
            const tasks: Task[] = []
            for (let i = 0; i < totalTasks; i++) {
                tasks.push({
                    taskId: `migration_${collectionName}_${i}_${crypto.randomBytes(4).toString('hex')}`,
                    type: 'migration',
                    collection: collectionName,
                    taskIndex: i,
                    totalTasks: totalTasks,
                    lastId: i === 0 ? null : 'SEQUENTIAL',
                    batchSize: appState.batchSize,
                    status: 'pending',
                    priority: 100,
                    migrationStartTime: migrationStartTime,
                    assignedTo: null,
                    attempts: 0
                })
            }
            
            if (tasks.length > 0) {
                await appState.tasksCollection.insertMany(tasks, { ordered: false })
                console.log(`[OK] Created ${tasks.length} migration tasks for ${collectionName}`)
            }
            
        } catch (error) {
            console.error(`[ERROR] Failed to process collection ${collectionName}:`)
            console.error('Details:', error instanceof Error ? error.message : error)
            console.error('Stack:', error instanceof Error ? error.stack : 'N/A')
            continue
        }
    }
    
    const totalMigrationTasks = await appState.tasksCollection.countDocuments({ type: 'migration' })
    console.log(`[OK] Generated ${totalMigrationTasks} total migration tasks for ${collections.length} collections`)
}

async function waitForMigrationAndCatchUp(): Promise<void> {
    console.log('[WAIT] Waiting for migration completion...')
    
    if (!appState.tasksCollection || !appState.progressCollection) {
        throw new Error('Task collections not initialized')
    }
    
    // Monitor migration progress
    while (true) {
        const pendingTasks = await appState.tasksCollection.countDocuments({
            type: 'migration',
            status: { $in: ['pending', 'assigned', 'running'] }
        })
        
        if (pendingTasks === 0) {
            console.log('[OK] All migration tasks completed')
            break
        }
        
        const taskStats = await appState.tasksCollection.aggregate([
            { $match: { type: 'migration' } },
            { $group: { _id: '$status', count: { $sum: 1 } } }
        ]).toArray()
        
        const statusCounts = taskStats.reduce((acc, stat) => {
            acc[stat._id] = stat.count
            return acc
        }, {} as Record<string, number>)
        
        console.log(`[INFO] ${pendingTasks} migration tasks remaining... Status breakdown: ${JSON.stringify(statusCounts)}`)
        await delay(10000)
    }
    
    // Mark collections as migration complete
    await appState.progressCollection.updateMany(
        { status: 'migration_in_progress' },
        { 
            $set: { 
                status: 'migration_complete',
                migrationCompletedAt: new Date() 
            } 
        }
    )
    
    // Perform catch-up synchronization
    console.log('[SYNC] Performing catch-up synchronization...')
    const collections = await appState.progressCollection.find({ status: 'migration_complete' }).toArray()
    
    for (const collection of collections) {
        await catchUpCollection(collection)
    }
    
    await appState.progressCollection.updateMany(
        { status: 'migration_complete' },
        { 
            $set: { 
                status: 'ready_for_realtime',
                catchUpCompletedAt: new Date() 
            } 
        }
    )
    
    console.log('[OK] Catch-up synchronization completed')
}

async function createIndexes(): Promise<void> {
    console.log('[INDEX] Creating indexes (coordinator exclusive)...')

    if (!appState.progressCollection) {
        throw new Error('Progress collection not initialized')
    }
    
    const collections = await appState.progressCollection.find({ 
        status: { $in: ['ready_for_realtime', 'active_sync'] }
    }).toArray()
    
    if (collections.length === 0) {
        console.log('[INDEX] No collections ready for index creation')
        return
    }
    
    for (const collection of collections) {
        await createCollectionIndexes(collection.collection)
    }
    
    await appState.progressCollection.updateMany(
        { status: { $in: ['ready_for_realtime', 'active_sync'] } },
        { 
            $set: { 
                indexesCompletedAt: new Date() 
            } 
        }
    )
    
    console.log('[OK] Index creation completed')
}

async function signalRealTimeSync(): Promise<void> {
    console.log('[SIGNAL] Signaling workers to start real-time sync...')
    
    if (!appState.progressCollection || !appState.tasksCollection) {
        throw new Error('Required collections not initialized')
    }
    
    // Clean up any duplicate real-time sync tasks first
    console.log('[CLEANUP] Removing duplicate real-time sync tasks...')
    const duplicateTasks = await appState.tasksCollection.aggregate([
        { $match: { type: 'realtime_sync' } },
        { $group: { _id: '$collection', count: { $sum: 1 }, tasks: { $push: '$taskId' } } },
        { $match: { count: { $gt: 1 } } }
    ]).toArray()
    
    for (const duplicate of duplicateTasks) {
        // Keep the first task, remove the others
        const tasksToRemove = duplicate.tasks.slice(1)
        await appState.tasksCollection.deleteMany({
            taskId: { $in: tasksToRemove }
        })
        console.log(`[CLEANUP] Removed ${tasksToRemove.length} duplicate real-time sync tasks for ${duplicate._id}`)
    }
    
    const collections = await appState.progressCollection.find({ status: 'ready_for_realtime' }).toArray()
    
    // Create ONE real-time sync task per collection (workers can join the same change stream)
    for (const collection of collections) {
        // Check if a real-time sync task already exists for this collection
        const existingTask = await appState.tasksCollection.findOne({
            type: 'realtime_sync',
            collection: collection.collection,
            status: { $in: ['pending', 'running'] }
        })
        
        if (!existingTask) {
            await appState.tasksCollection.insertOne({
                taskId: `realtime_${collection.collection}_${crypto.randomBytes(4).toString('hex')}`,
                type: 'realtime_sync',
                collection: collection.collection,
                status: 'pending',
                priority: 90,
                assignedTo: null,
                attempts: 0,
                persistent: true
            })
            
            console.log(`[OK] Created real-time sync task for ${collection.collection}`)
        } else {
            console.log(`[INFO] Real-time sync task already exists for ${collection.collection}`)
        }
    }
    
    const totalRealtimeTasks = await appState.tasksCollection.countDocuments({
        type: 'realtime_sync',
        status: { $in: ['pending', 'running'] }
    })
    console.log(`[OK] ${totalRealtimeTasks} real-time sync tasks active`)
}

async function monitorSystem(): Promise<void> {
    console.log('[MONITOR] Monitoring system health...')
    
    const monitorInterval = setInterval(async () => {
        try {
            await cleanupDeadWorkers()
            await checkOrphanedRealTimeSync()
            await reassignFailedTasks()
            await logSystemStats()
        } catch (error) {
            console.error('Error in system monitoring:', error)
        }
    }, 30000)
    
    // Keep coordinator running
    return new Promise<void>((resolve, reject) => {
        appState.emitter.on('shutdown', () => {
            clearInterval(monitorInterval)
            resolve()
        })
        
        appState.emitter.on('error', (error) => {
            clearInterval(monitorInterval)
            reject(error)
        })
    })
}

async function cleanupDeadWorkers(): Promise<void> {
    const deadThreshold = new Date(Date.now() - 90 * 1000)
    
    if (!appState.workersCollection || !appState.tasksCollection) {
        return
    }
    
    const deadWorkers = await appState.workersCollection.find({
        mode: 'worker',
        lastHeartbeat: { $lt: deadThreshold },
        status: { $ne: 'dead' }
    }).toArray()
    
    if (deadWorkers.length > 0) {
        console.log(`[CLEANUP] Found ${deadWorkers.length} dead workers`)
    }
    
    for (const worker of deadWorkers) {
        console.log(`[CLEANUP] Cleaning up dead worker: ${worker.workerId}`)
        
        // Mark worker as dead
        await appState.workersCollection.updateOne(
            { workerId: worker.workerId },
            {
                $set: {
                    status: 'dead',
                    diedAt: new Date()
                }
            }
        )
        
        // Reassign any tasks assigned to this worker
        const result = await appState.tasksCollection.updateMany(
            {
                assignedTo: worker.workerId,
                status: { $in: ['assigned', 'running'] },
                persistent: { $ne: true }
            },
            {
                $set: {
                    status: 'pending',
                    assignedTo: null,
                    reassignedAt: new Date(),
                    reassignReason: `Worker ${worker.workerId} died`
                }
            }
        )
        
        if (result.modifiedCount > 0) {
            console.log(`[CLEANUP] Reassigned ${result.modifiedCount} tasks from dead worker ${worker.workerId}`)
        }
        
        // Handle persistent real-time sync tasks differently
        const persistentTasks = await appState.tasksCollection.find({
            assignedTo: worker.workerId,
            status: { $in: ['assigned', 'running'] },
            persistent: true,
            type: 'realtime_sync'
        }).toArray()
        
        for (const task of persistentTasks) {
            console.log(`[CLEANUP] Creating replacement real-time sync task for ${task.collection} (replacing dead worker ${worker.workerId})`)
            
            // Create a new real-time sync task to replace the dead one
            await appState.tasksCollection.insertOne({
                taskId: `realtime_${task.collection}_replacement_${crypto.randomBytes(4).toString('hex')}`,
                type: 'realtime_sync',
                collection: task.collection,
                status: 'pending',
                priority: 90,
                assignedTo: null,
                attempts: 0,
                persistent: true,
                metadata: { replacesTask: task.taskId, replacedWorker: worker.workerId }
            })
            
            // Mark the old task as failed
            await appState.tasksCollection.updateOne(
                { taskId: task.taskId },
                {
                    $set: {
                        status: 'failed',
                        failedAt: new Date(),
                        error: `Worker ${worker.workerId} died`,
                        reassignReason: 'Worker died - replacement task created'
                    }
                }
            )
        }
    }
}

async function checkOrphanedRealTimeSync(): Promise<void> {
    if (!appState.progressCollection || !appState.workersCollection || !appState.tasksCollection) {
        return
    }
    
    // Find collections that are in active_sync and check their workers
    const activeCollections = await appState.progressCollection.find({
        status: 'active_sync'
    }).toArray()
    
    for (const collection of activeCollections) {
        if (!collection.realtimeSyncWorkers) continue
        
        const validWorkers = []
        
        for (const workerId of collection.realtimeSyncWorkers) {
            const worker = await appState.workersCollection.findOne({ workerId })
            
            if (worker && worker.status === 'active' && 
                (Date.now() - worker.lastHeartbeat.getTime()) < 90000) {
                validWorkers.push(workerId)
            } else {
                console.log(`[ORPHAN] Worker ${workerId} for collection ${collection.collection} is no longer active`)
            }
        }
        
        // Update the collection with only valid workers
        if (validWorkers.length !== collection.realtimeSyncWorkers.length) {
            await appState.progressCollection.updateOne(
                { collection: collection.collection },
                { $set: { realtimeSyncWorkers: validWorkers } }
            )
        }
        
        // Check if there's any active real-time sync task for this collection
        const activeRealtimeTask = await appState.tasksCollection.findOne({
            type: 'realtime_sync',
            collection: collection.collection,
            status: { $in: ['pending', 'running'] }
        })
        
        if (!activeRealtimeTask) {
            console.log(`[ORPHAN] Collection ${collection.collection} has no active real-time sync task, creating one`)
            await createMissingRealtimeTasks(collection.collection, 1)
        } else {
            console.log(`[ORPHAN] Collection ${collection.collection} has active real-time sync task: ${activeRealtimeTask.taskId}`)
        }
    }
}

async function createMissingRealtimeTasks(collectionName: string, count: number = 1): Promise<void> {
    if (!appState.tasksCollection) {
        return
    }
    
    // Always only create 1 real-time sync task per collection
    const existingTask = await appState.tasksCollection.findOne({
        type: 'realtime_sync',
        collection: collectionName,
        status: { $in: ['pending', 'running'] }
    })
    
    if (existingTask) {
        console.log(`[TASK CREATE] Real-time sync task already exists for ${collectionName}: ${existingTask.taskId}`)
        return
    }
    
    console.log(`[TASK CREATE] Creating 1 real-time sync task for ${collectionName}`)
    
    await appState.tasksCollection.insertOne({
        taskId: `realtime_${collectionName}_${crypto.randomBytes(4).toString('hex')}`,
        type: 'realtime_sync',
        collection: collectionName,
        status: 'pending',
        priority: 90,
        assignedTo: null,
        attempts: 0,
        persistent: true,
        metadata: { reason: 'Single task recovery' }
    })
}

async function reassignFailedTasks(): Promise<void> {
    if (!appState.tasksCollection) {
        return
    }
    
    await appState.tasksCollection.updateMany(
        {
            status: 'failed',
            attempts: { $lt: 3 },
            persistent: { $ne: true }
        },
        { 
            $set: { 
                status: 'pending',
                assignedTo: null 
            } 
        }
    )
}

async function logSystemStats(): Promise<void> {
    const [taskStats, workerStats, progressStats] = await Promise.all([
        getTaskStats(),
        getWorkerStats(),
        getProgressStats()
    ])
    
    console.log(`[STATS] Tasks: ${taskStats.running}R/${taskStats.pending}P/${taskStats.completed}C/${taskStats.failed}F | Workers: ${workerStats.active}A/${workerStats.dead}D | Progress: ${progressStats.completed}/${progressStats.total} collections`)
}

// ===== WORKER FUNCTIONS =====
export async function runWorker(): Promise<void> {
    if (appState.mode !== 'worker') {
        throw new Error('Not in worker mode')
    }
    
    console.log('>> Starting WORKER workflow...')
    appState.isRunning = true
    
    try {
        startTaskPolling()
        
        // Keep worker running until shutdown
        return new Promise<void>((resolve, reject) => {
            appState.emitter.on('shutdown', () => {
                stopTaskPolling()
                resolve()
            })
            
            appState.emitter.on('error', (error) => {
                stopTaskPolling()
                reject(error)
            })
        })
    } catch (error) {
        console.error('Worker error:')
        console.error('Details:', error instanceof Error ? error.message : error)
        console.error('Stack:', error instanceof Error ? error.stack : 'N/A')
        throw error
    }
}

function startTaskPolling(): void {
    console.log('[POLL] Starting task polling...')
    
    appState.taskPollingInterval = setInterval(async () => {
        try {
            const task = await claimTask()
            if (task) {
                await executeTask(task)
            }
        } catch (error) {
            console.error('Error in task polling:')
            console.error('Details:', error instanceof Error ? error.message : error)
        }
    }, 5000)
}

function stopTaskPolling(): void {
    if (appState.taskPollingInterval) {
        clearInterval(appState.taskPollingInterval)
        appState.taskPollingInterval = null
    }
}

async function claimTask(): Promise<Task | null> {
    try {
        if (!appState.tasksCollection || !appState.workerId) {
            return null
        }
        
        // Check if this worker already has a running real-time sync task
        const currentRealtimeTask = await appState.tasksCollection.findOne({
            type: 'realtime_sync',
            assignedTo: appState.workerId,
            status: { $in: ['assigned', 'running'] }
        })
        
        let query: any = {
            status: 'pending',
            $or: [
                { assignedTo: null },
                { assignedTo: { $exists: false } }
            ]
        }
        
        // If worker already has a real-time sync task, exclude all real-time sync tasks from claiming
        if (currentRealtimeTask) {
            query.type = { $ne: 'realtime_sync' }
            console.log(`[CLAIM] Worker ${appState.workerId} already has real-time sync task ${currentRealtimeTask.taskId}, skipping real-time sync tasks`)
        }
        
        // Try to claim a task atomically
        const result = await appState.tasksCollection.findOneAndUpdate(
            query,
            {
                $set: {
                    status: 'assigned',
                    assignedTo: appState.workerId,
                    assignedAt: new Date(),
                    lastHeartbeat: new Date()
                },
                $inc: { attempts: 1 }
            },
            {
                sort: { priority: -1, _id: 1 },
                returnDocument: 'after'
            }
        )
        
        if (result) {
            console.log(`[CLAIM] Claimed task: ${result.taskId} (${result.type} for ${result.collection})`)
            return result
        }
        
        return null
    } catch (error) {
        console.error('Error claiming task:')
        console.error('Details:', error instanceof Error ? error.message : error)
        return null
    }
}

async function executeTask(task: Task): Promise<void> {
    console.log(`[EXEC] Executing ${task.type} task for ${task.collection} (${task.taskId})`)
    
    try {
        if (!appState.tasksCollection) {
            throw new Error('Tasks collection not initialized')
        }
        
        // Mark task as running
        await appState.tasksCollection.updateOne(
            { taskId: task.taskId },
            {
                $set: {
                    status: 'running',
                    startedAt: new Date(),
                    lastHeartbeat: new Date()
                }
            }
        )
        
        // Execute based on task type
        switch (task.type) {
            case 'migration':
                await executeMigrationTask(task)
                console.log(`[OK] Task ${task.taskId} completed successfully`)
                break
            case 'realtime_sync':
                // Real-time sync tasks never complete - they run until shutdown
                console.log(`[REALTIME] Task ${task.taskId} starting indefinite real-time sync`)
                await executeRealtimeSyncTask(task)
                // This should only be reached if the application is shutting down
                console.log(`[REALTIME] Task ${task.taskId} stopped (application shutdown)`)
                break
            case 'catchup':
                await executeCatchupTask(task)
                console.log(`[OK] Task ${task.taskId} completed successfully`)
                break
            default:
                throw new Error(`Unknown task type: ${task.type}`)
        }
        
    } catch (error) {
        console.error(`[ERROR] Task ${task.taskId} failed:`)
        console.error('Details:', error instanceof Error ? error.message : error)
        console.error('Stack:', error instanceof Error ? error.stack : 'N/A')
        
        // Mark task as failed
        if (appState.tasksCollection) {
            await appState.tasksCollection.updateOne(
                { taskId: task.taskId },
                {
                    $set: {
                        status: 'failed',
                        failedAt: new Date(),
                        error: error instanceof Error ? error.message : String(error)
                    }
                }
            )
        }
        
        throw error
    }
}

async function executeMigrationTask(task: Task): Promise<void> {
    const { collection, lastId, batchSize: taskBatchSize, taskIndex, totalTasks } = task
    
    if (!appState.sourceDb || !appState.destDb || !appState.progressCollection || !appState.tasksCollection) {
        throw new Error('Required database connections not established')
    }
    
    const sourceCollection = appState.sourceDb.collection(collection)
    const destCollection = appState.destDb.collection(collection)
    const effectiveBatchSize = taskBatchSize || appState.batchSize
    
    console.log(`[MIGRATE] Starting migration for ${collection} (task ${(taskIndex || 0) + 1}/${totalTasks || 1})`)
    
    let currentLastId = lastId
    if (lastId === 'SEQUENTIAL') {
        currentLastId = await determineTaskStartingPosition(collection, taskIndex || 0)
    }
    let totalProcessed = 0
    
    const maxDocumentsPerTask = 2000
    
    while (totalProcessed < maxDocumentsPerTask) {
        const query = currentLastId ? { _id: { $gt: currentLastId } } : {}
        const remainingInTask = maxDocumentsPerTask - totalProcessed
        const currentBatchSize = Math.min(effectiveBatchSize, remainingInTask)
        
        const documents = await sourceCollection
            .find(query)
            .sort({ _id: 1 })
            .limit(currentBatchSize)
            .toArray()
        
        if (documents.length === 0) {
            console.log(`[MIGRATE] No more documents for ${collection} (task ${task.taskId})`)
            break
        }
        
        console.log(`[MIGRATE] Processing batch of ${documents.length} documents for ${collection}`)
        
        for (let i = 0; i < documents.length; i += appState.batchSize) {
            const batch = documents.slice(i, i + appState.batchSize)
            await processBatch(destCollection, batch, collection)
        }
        
        totalProcessed += documents.length
        currentLastId = documents[documents.length - 1]._id
        
        await appState.tasksCollection.updateOne(
            { taskId: task.taskId },
            {
                $set: {
                    lastId: currentLastId,
                    lastUpdated: new Date(),
                    lastHeartbeat: new Date()
                }
            }
        )
        
        await appState.progressCollection.updateOne(
            { collection: collection },
            {
                $inc: { migratedCount: documents.length },
                $set: { 
                    status: 'migration_in_progress',
                    lastUpdated: new Date()
                }
            }
        )
        
        if (totalProcessed >= maxDocumentsPerTask) {
            console.log(`[MIGRATE] Task ${task.taskId} reached limit: ${totalProcessed} documents`)
            break
        }
    }
    
    await appState.tasksCollection.updateOne(
        { taskId: task.taskId },
        {
            $set: {
                status: 'completed',
                completedAt: new Date()
            }
        }
    )
    
    await appState.progressCollection.updateOne(
        { collection: collection },
        {
            $inc: { completedTasks: 1 },
            $set: { lastUpdated: new Date() }
        }
    )
    
    const progress = await appState.progressCollection.findOne({ collection: collection })
    
    if (progress && progress.completedTasks >= progress.totalTasks) {
        console.log(`[COMPLETE] All migration tasks completed for ${collection}`)
        
        await appState.progressCollection.updateOne(
            { collection: collection },
            {
                $set: {
                    status: 'migration_complete',
                    migrationCompletedAt: new Date()
                }
            }
        )
    }
}

async function determineTaskStartingPosition(collectionName: string, taskIndex: number): Promise<any> {
    if (!appState.tasksCollection || !appState.sourceDb) {
        return null
    }
    
    if (taskIndex === 0) {
        return null
    }
    
    const lastCompletedTask = await appState.tasksCollection.findOne({
        collection: collectionName,
        type: 'migration',
        status: 'completed',
        lastId: { $exists: true, $ne: null }
    }, {
        sort: { taskIndex: -1 }
    })
    
    if (lastCompletedTask && lastCompletedTask.lastId) {
        console.log(`[SEQUENTIAL] Task ${taskIndex} starting after: ${JSON.stringify(lastCompletedTask.lastId)}`)
        return lastCompletedTask.lastId
    }
    
    console.log(`[SEQUENTIAL] Task ${taskIndex} starting from beginning`)
    return null
}

async function executeRealtimeSyncTask(task: Task): Promise<void> {
    const { collection } = task
    
    console.log(`[REALTIME] Starting real-time sync for ${collection}`)
    
    if (!appState.progressCollection) {
        throw new Error('Progress collection not initialized')
    }
    
    const progress = await appState.progressCollection.findOne({ collection })
    
    if (progress?.status === 'active_sync') {
        console.log(`[REALTIME] Collection ${collection} already has active sync - joining as additional worker`)
    } else {
        await appState.progressCollection.updateOne(
            { collection },
            {
                $set: {
                    status: 'active_sync',
                    realtimeSyncStartedAt: new Date()
                }
            }
        )
        
        // Start change stream
        await startCollectionChangeStream(collection)
    }
    
    // Register this worker
    await appState.progressCollection.updateOne(
        { collection },
        {
            $addToSet: { realtimeSyncWorkers: appState.workerId! },
            $set: { lastUpdated: new Date() }
        }
    )
    
    console.log(`[OK] Real-time sync active for ${collection}`)
    
    if (!appState.lagMonitorInterval) {
        startLagMonitoring()
    }
    
    appState.lagMetrics.set(collection, {
        lastChangeTime: Date.now(),
        lastProcessedTime: Date.now(),
        currentLag: 0,
        averageLag: 0,
        maxLag: 0,
        minLag: Infinity,
        processingTime: 0,
        changeCount: 0,
        errorCount: 0,
        lagHistory: [],
        lastLogTime: Date.now()
    })
    
    console.log(`[LAG] Initialized lag monitoring for ${collection}`)
    
    // Keep task running until shutdown
    while (appState.isRunning && appState.changeStreams.has(collection)) {
        await delay(10000)
    }
}

async function executeCatchupTask(task: Task): Promise<void> {
    const { collection, query } = task
    
    console.log(`[CATCHUP] Executing catchup task for ${collection}`)
    
    if (!appState.sourceDb || !appState.destDb || !appState.tasksCollection) {
        throw new Error('Required database connections not established')
    }
    
    try {
        const sourceCollection = appState.sourceDb.collection(collection)
        const destCollection = appState.destDb.collection(collection)
        
        const cursor = sourceCollection.find(query || {}).sort({ _id: 1 })
        let processedCount = 0
        const batch: any[] = []
        
        for await (const doc of cursor) {
            batch.push(doc)
            
            if (batch.length >= appState.batchSize) {
                await processBatch(destCollection, batch, collection)
                processedCount += batch.length
                batch.length = 0
            }
        }
        
        if (batch.length > 0) {
            await processBatch(destCollection, batch, collection)
            processedCount += batch.length
        }
        
        console.log(`[OK] Catchup completed for ${collection}: ${processedCount} documents`)
        
        await appState.tasksCollection.updateOne(
            { taskId: task.taskId },
            {
                $set: {
                    status: 'completed',
                    completedAt: new Date()
                }
            }
        )
        
    } catch (error) {
        console.error(`[ERROR] Catchup failed for ${collection}:`, error)
        throw error
    }
}

async function getCollectionsToSync(): Promise<string[]> {
    if (!appState.sourceDb) {
        throw new Error('Source database not connected')
    }
    
    const existingCollections = await appState.sourceDb.listCollections().toArray()
    const existingCollectionNames = existingCollections.map(col => col.name)
    
    const validCollections = appState.collectionsToSync.filter(
        colName => existingCollectionNames.includes(colName)
    )
    
    if (validCollections.length !== appState.collectionsToSync.length) {
        const missing = appState.collectionsToSync.filter(col => !validCollections.includes(col))
        console.warn(`Collections not found in source: ${missing.join(', ')}`)
    }
    
    return validCollections
}

async function processBatch(destCollection: any, batch: any[], collectionName: string): Promise<void> {
    let retries = 0
    
    while (retries < appState.maxRetries) {
        try {
            const operations = batch.map(doc => ({
                replaceOne: {
                    filter: { _id: doc._id },
                    replacement: doc,
                    upsert: true
                }
            }))
            
            await destCollection.bulkWrite(operations, { ordered: false })
            return
        } catch (error) {
            retries++
            console.error(`Batch operation failed (attempt ${retries}/${appState.maxRetries}):`, error)
            if (retries >= appState.maxRetries) throw error
            await delay(appState.retryDelay)
        }
    }
}

async function catchUpCollection(collectionProgress: ProgressDocument): Promise<void> {
    const { collection, migrationStartTime } = collectionProgress
    console.log(`[CATCHUP] Catch-up sync for ${collection} from ${migrationStartTime}`)
    
    if (!appState.sourceDb || !appState.destDb) {
        throw new Error('Database connections not established')
    }
    
    const sourceCollection = appState.sourceDb.collection(collection)
    const destCollection = appState.destDb.collection(collection)
    
    // Use _id instead of createdAt for catch-up query
    const catchUpQuery = { _id: { $gte: ObjectId.createFromTime(Math.floor(migrationStartTime.getTime() / 1000)) } }
    
    const cursor = sourceCollection.find(catchUpQuery).sort({ _id: 1 })
    let catchUpCount = 0
    let batch = []
    
    for await (const doc of cursor) {
        batch.push(doc)
        
        if (batch.length >= appState.batchSize) {
            await processBatch(destCollection, batch, collection)
            catchUpCount += batch.length
            batch.length = 0
        }
    }
    
    if (batch.length > 0) {
        await processBatch(destCollection, batch, collection)
        catchUpCount += batch.length
    }
    
    console.log(`[OK] Catch-up completed for ${collection}: ${catchUpCount} documents`)
}

async function createCollectionIndexes(collectionName: string): Promise<void> {
    console.log(`[INDEX] Creating indexes for ${collectionName}`)

    try {
        if (!appState.sourceDb || !appState.destDb) {
            throw new Error('Database connections not established')
        }
        
        const sourceCollection = appState.sourceDb.collection(collectionName)
        const destCollection = appState.destDb.collection(collectionName)
        
        // Get indexes from source
        const sourceIndexes = await sourceCollection.listIndexes().toArray()
        
        console.log(`[INDEX] Found ${sourceIndexes.length} indexes in source ${collectionName}`)
        
        for (const index of sourceIndexes) {
            // Skip the default _id index
            if (index.name === '_id_') continue
            
            try {
                console.log(`[INDEX] Creating index ${index.name} on ${collectionName}`)
                
                const indexSpec = { ...index }
                delete indexSpec.v
                delete indexSpec.ns
                
                await destCollection.createIndex(indexSpec.key, {
                    name: indexSpec.name,
                    background: true,
                    ...indexSpec
                })
                
                console.log(`[OK] Index ${index.name} created on ${collectionName}`)
            } catch (error) {
                console.error(`[ERROR] Failed to create index ${index.name} on ${collectionName}:`, error)
            }
        }
        
        console.log(`[OK] Index creation completed for ${collectionName}`)
        
    } catch (error) {
        console.error(`[ERROR] Index creation failed for ${collectionName}:`, error)
        throw error
    }
}

async function startCollectionChangeStream(collectionName: string): Promise<void> {
    try {
        if (!appState.sourceDb || !appState.destDb) {
            throw new Error('Database connections not established')
        }
        
        const sourceCollection = appState.sourceDb.collection(collectionName)
        const destCollection = appState.destDb.collection(collectionName)
        
        // Get resume token if available
        const resumeToken = await getResumeToken(collectionName)
        
        const changeStreamOptions: any = {
            fullDocument: 'updateLookup'
        }
        
        if (resumeToken) {
            changeStreamOptions.resumeAfter = resumeToken
            console.log(`[STREAM] Starting change stream for ${collectionName} from resume token`)
        } else {
            console.log(`[STREAM] Starting change stream for ${collectionName} from now`)
        }
        
        const changeStream = sourceCollection.watch([], changeStreamOptions)
        appState.changeStreams.set(collectionName, changeStream)
        
        changeStream.on('change', async (change) => {
            await handleChangeEvent(change, destCollection, collectionName)
        })
        
        changeStream.on('error', (error) => {
            console.error(`[ERROR] Change stream error for ${collectionName}:`, error)
        })
        
        console.log(`[OK] Change stream started for ${collectionName}`)
        
    } catch (error) {
        console.error(`[ERROR] Failed to start change stream for ${collectionName}:`)
        console.error('Details:', error instanceof Error ? error.message : error)
        console.error('Stack:', error instanceof Error ? error.stack : 'N/A')
        throw error
    }
}

async function handleChangeEvent(change: any, destCollection: any, collectionName: string): Promise<void> {
    const startTime = Date.now()
    
    try {
        // Extract timestamp from clusterTime - handle different MongoDB driver versions
        let sourceTime = Date.now()
        if (change.clusterTime) {
            try {
                // Try new format first (Timestamp object)
                if (typeof change.clusterTime.getTimestamp === 'function') {
                    sourceTime = change.clusterTime.getTimestamp().getTime()
                } else if (change.clusterTime.high && change.clusterTime.low) {
                    // Handle BSON Timestamp format
                    sourceTime = change.clusterTime.high * 1000 + change.clusterTime.low
                } else if (typeof change.clusterTime === 'number') {
                    sourceTime = change.clusterTime
                } else {
                    console.log(`[STREAM] Unknown clusterTime format:`, change.clusterTime)
                }
            } catch (timeError) {
                console.log(`[STREAM] Error parsing clusterTime, using current time:`, timeError)
                sourceTime = Date.now()
            }
        }
        
        switch (change.operationType) {
            case 'insert':
            case 'update':
            case 'replace':
                if (change.fullDocument) {
                    await destCollection.replaceOne(
                        { _id: change.fullDocument._id },
                        change.fullDocument,
                        { upsert: true }
                    )
                }
                break
                
            case 'delete':
                await destCollection.deleteOne({ _id: change.documentKey._id })
                break
                
            default:
                console.log(`[STREAM] Unhandled operation type: ${change.operationType}`)
                break
        }
        
        const processedTime = Date.now()
        const totalLag = processedTime - sourceTime
        const processingTime = processedTime - startTime
        
        updateLagMetrics(collectionName, sourceTime, processedTime, totalLag, processingTime, false)
        
        // Update resume token
        if (change._id) {
            await updateResumeToken(collectionName, change._id)
        }
        
    } catch (error) {
        console.error(`[ERROR] Change event processing failed for ${collectionName}:`, error)
        
        const processedTime = Date.now()
        updateLagMetrics(collectionName, startTime, processedTime, processedTime - startTime, 0, true)
        
        throw error
    }
}

function updateLagMetrics(collectionName: string, sourceTime: number, processedTime: number, totalLag: number, processingTime: number = 0, isError: boolean = false): void {
    const currentMetrics = appState.lagMetrics.get(collectionName) || {
        lastChangeTime: 0,
        lastProcessedTime: 0,
        currentLag: 0,
        averageLag: 0,
        maxLag: 0,
        minLag: Infinity,
        processingTime: 0,
        changeCount: 0,
        errorCount: 0,
        lagHistory: [],
        lastLogTime: 0
    }
    
    currentMetrics.lastChangeTime = sourceTime
    currentMetrics.lastProcessedTime = processedTime
    currentMetrics.currentLag = totalLag
    currentMetrics.processingTime = processingTime
    currentMetrics.changeCount++
    
    if (isError) {
        currentMetrics.errorCount++
    }
    
    if (totalLag > currentMetrics.maxLag) {
        currentMetrics.maxLag = totalLag
    }
    
    if (totalLag < currentMetrics.minLag) {
        currentMetrics.minLag = totalLag
    }
    
    currentMetrics.lagHistory.push({
        timestamp: processedTime,
        lag: totalLag,
        processingTime: processingTime,
        isError: isError
    })
    
    if (currentMetrics.lagHistory.length > 100) {
        currentMetrics.lagHistory.shift()
    }
    
    // Calculate rolling average
    const recentLags = currentMetrics.lagHistory.slice(-10).map(h => h.lag)
    currentMetrics.averageLag = recentLags.reduce((sum, lag) => sum + lag, 0) / recentLags.length
    
    appState.lagMetrics.set(collectionName, currentMetrics)
}

function startLagMonitoring(): void {
    appState.lagMonitorInterval = setInterval(async () => {
        try {
            const lagReport: any = {
                timestamp: new Date(),
                collections: {}
            }
            
            for (const [collection, metrics] of appState.lagMetrics) {
                if (Date.now() - metrics.lastLogTime > 30000) {
                    lagReport.collections[collection] = {
                        currentLag: metrics.currentLag,
                        averageLag: metrics.averageLag,
                        maxLag: metrics.maxLag,
                        changeCount: metrics.changeCount,
                        errorCount: metrics.errorCount
                    }
                    
                    metrics.lastLogTime = Date.now()
                }
            }
            
            if (Object.keys(lagReport.collections).length > 0) {
                console.log(`[LAG] ${JSON.stringify(lagReport)}`)
                await storeLagMetrics(lagReport)
            }
            
        } catch (error) {
            console.error('[ERROR] Lag monitoring failed:', error)
        }
    }, 30000)
}

async function storeLagMetrics(lagReport: any): Promise<void> {
    try {
        if (appState.progressDb) {
            const metricsCollection = appState.progressDb.collection('lag_metrics')
            await metricsCollection.insertOne({
                ...lagReport,
                workerId: appState.workerId,
                createdAt: new Date()
            })
        }
    } catch (error) {
        console.error('Error storing lag metrics:', error)
    }
}

function stopLagMonitoring(): void {
    if (appState.lagMonitorInterval) {
        clearInterval(appState.lagMonitorInterval)
        appState.lagMonitorInterval = null
    }
}

async function getResumeToken(collectionName: string): Promise<any> {
    if (!appState.progressCollection) return null
    
    const progress = await appState.progressCollection.findOne({ collection: collectionName })
    return progress?.resumeToken || null
}

async function updateResumeToken(collectionName: string, resumeToken: any): Promise<void> {
    if (!appState.progressCollection) return
    
    await appState.progressCollection.updateOne(
        { collection: collectionName },
        { $set: { resumeToken, lastUpdated: new Date() } }
    )
}

async function getTaskStats(): Promise<any> {
    if (!appState.tasksCollection) return { running: 0, pending: 0, completed: 0, failed: 0 }
    
    const stats = await appState.tasksCollection.aggregate([
        { $group: { _id: '$status', count: { $sum: 1 } } }
    ]).toArray()
    
    return stats.reduce((acc, stat) => {
        acc[stat._id] = stat.count
        return acc
    }, { running: 0, pending: 0, completed: 0, failed: 0 })
}

async function getWorkerStats(): Promise<any> {
    if (!appState.workersCollection) return { active: 0, dead: 0 }
    
    const stats = await appState.workersCollection.aggregate([
        { $group: { _id: '$status', count: { $sum: 1 } } }
    ]).toArray()
    
    return stats.reduce((acc, stat) => {
        acc[stat._id] = stat.count
        return acc
    }, { active: 0, dead: 0 })
}

async function getProgressStats(): Promise<any> {
    if (!appState.progressCollection) return { completed: 0, total: 0 }
    
    const total = await appState.progressCollection.countDocuments({})
    const completed = await appState.progressCollection.countDocuments({
        status: { $in: ['ready_for_realtime', 'active_sync'] }
    })
    
    return { completed, total }
}

export async function getSystemStatus(): Promise<any> {
    const [taskStats, workerStats, progressStats] = await Promise.all([
        getTaskStats(),
        getWorkerStats(),
        getProgressStats()
    ])
    
    const lagMetricsSnapshot: any = {}
    for (const [collection, metrics] of appState.lagMetrics) {
        lagMetricsSnapshot[collection] = {
            currentLag: metrics.currentLag,
            averageLag: metrics.averageLag,
            maxLag: metrics.maxLag,
            changeCount: metrics.changeCount,
            errorCount: metrics.errorCount
        }
    }
    
    return {
        mode: appState.mode,
        workerId: appState.workerId,
        isRunning: appState.isRunning,
        tasks: taskStats,
        workers: workerStats,
        progress: progressStats,
        lagMetrics: lagMetricsSnapshot,
        collections: appState.collectionsToSync,
        changeStreams: Array.from(appState.changeStreams.keys())
    }
}

function delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms))
}

function setupShutdownHandlers(): void {
    ['SIGINT', 'SIGTERM', 'SIGQUIT'].forEach(signal => {
        process.on(signal, async () => {
            console.log(`Received ${signal}, shutting down gracefully...`)
            await cleanup()
            process.exit(0)
        })
    })
}

export async function cleanup(): Promise<void> {
    console.log('Cleaning up resources...')
    
    appState.isRunning = false
    
    // Stop intervals
    if (appState.taskPollingInterval) {
        clearInterval(appState.taskPollingInterval)
        appState.taskPollingInterval = null
    }
    
    if (appState.heartbeatInterval) {
        clearInterval(appState.heartbeatInterval)
        appState.heartbeatInterval = null
    }
    
    stopLagMonitoring()
    
    // Close change streams
    for (const [collection, changeStream] of appState.changeStreams) {
        try {
            await changeStream.close()
            console.log(`Closed change stream for ${collection}`)
        } catch (error) {
            console.error(`Error closing change stream for ${collection}:`, error)
        }
    }
    appState.changeStreams.clear()
    
    // Close database connections
    if (appState.sourceClient) {
        await appState.sourceClient.close()
        console.log('Closed source database connection')
    }
    
    if (appState.destClient) {
        await appState.destClient.close()
        console.log('Closed destination database connection')
    }
    
    // Mark worker as offline
    if (appState.workersCollection && appState.workerId) {
        try {
            await appState.workersCollection.updateOne(
                { workerId: appState.workerId },
                {
                    $set: {
                        status: 'offline',
                        shutdownAt: new Date()
                    }
                }
            )
            console.log(`Marked worker ${appState.workerId} as offline`)
        } catch (error) {
            console.error('Error updating worker status:', error)
        }
    }
    
    appState.emitter.emit('shutdown')
    console.log('Cleanup completed')
}

export { appState, setupShutdownHandlers }