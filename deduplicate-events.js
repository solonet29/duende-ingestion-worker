
require('dotenv').config({ path: './.env' });
const { connectToDatabase } = require('../duende-api-next/lib/database');

async function deduplicateEvents() {
    try {
        const db = await connectToDatabase();
        const eventsCollection = db.collection('events');

        const duplicates = await eventsCollection.aggregate([
            {
                $group: {
                    _id: { date: "$date", artist: "$artist" },
                    duplicates: { $push: "$_id" },
                    count: { $sum: 1 }
                }
            },
            {
                $match: {
                    count: { $gt: 1 }
                }
            }
        ]).toArray();

        if (duplicates.length === 0) {
            console.log('No duplicate events found.');
            return;
        }

        console.log(`Found ${duplicates.length} sets of duplicate events.`);

        let deletedCount = 0;
        for (const group of duplicates) {
            // Keep the first one, delete the rest
            const idsToDelete = group.duplicates.slice(1);
            const result = await eventsCollection.deleteMany({ _id: { $in: idsToDelete } });
            deletedCount += result.deletedCount;
            console.log(`Deleted ${result.deletedCount} duplicates for artist "${group._id.artist}" on date "${group._id.date}".`);
        }

        console.log(`
Total duplicate events deleted: ${deletedCount}`);

    } catch (error) {
        console.error('Error de-duplicating events:', error);
    } finally {
        // It's important to close the connection, but the connectToDatabase function doesn't return a client to close.
        // Assuming the connection is managed elsewhere or the script is short-lived.
    }
}

deduplicateEvents();
