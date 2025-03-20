const fs = require('fs');
const path = require('path');
const { MongoClient } = require('mongodb');
const xml2js = require('xml2js');
require('dotenv').config();
const he = require('he'); // Per decodificar les entitats HTML
const winston = require('winston');

// Ruta al fitxer XML
const xmlFilePath = path.join(__dirname, '../../data/Posts.xml');

// Configuració de logger
const logger = winston.createLogger({
  level: 'info',
  transports: [
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        winston.format.simple()
      )
    }),
    new winston.transports.File({ filename: '../data/logs/exercici1.log' })
  ]
});

// Funció per llegir i analitzar el fitxer XML
async function parseXMLFile(filePath) {
  try {
    const xmlData = fs.readFileSync(filePath, 'utf-8');
    const parser = new xml2js.Parser({ 
      explicitArray: false,
      mergeAttrs: true
    });
    
    return new Promise((resolve, reject) => {
      parser.parseString(xmlData, (err, result) => {
        if (err) {
          logger.error('Error llegint o analitzant el fitxer XML:', err);
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  } catch (error) {
    logger.error('Error llegint o analitzant el fitxer XML:', error);
    throw error;
  }
}

// Funció per processar les dades i transformar-les a un format més adequat per MongoDB
function processAnimeData(data) {
  const posts = Array.isArray(data.posts.row) ? data.posts.row : [data.posts.row];
  
  return posts.map(post => {
    if (parseInt(post.ViewCount) > 20000) {
      // Decodificar entitats HTML en el cos i les etiquetes
      const decodedBody = he.decode(post.Body);
      const decodedTags = he.decode(post.Tags);
  
      // Retornar el document processat
      return {
        question: {
          Id: post.Id,
          PostTypeId: post.PostTypeId,
          AcceptedAnswerId: post.AcceptedAnswerId,
          CreationDate: post.CreationDate,
          Score: post.Score,
          ViewCount: post.ViewCount,
          Body: decodedBody, // Aquí hem decodificat el cos
          OwnerUserId: post.OwnerUserId,
          LastActivityDate: post.LastActivityDate,
          Title: post.Title,
          Tags: decodedTags, // També decodifiquem les etiquetes
          AnswerCount: post.AnswerCount,
          CommentCount: post.CommentCount,
          ContentLicense: post.ContentLicense
        }
      };
    }
    return null;
  }).filter(post => post !== null);
}

// Funció principal per carregar les dades a MongoDB
async function loadDataToMongoDB() {
  const uri = process.env.MONGODB_URI || 'mongodb://root:password@localhost:27017/';
  const client = new MongoClient(uri);
  
  try {
    await client.connect();
    logger.info('Connectat a MongoDB');
    
    const database = client.db('animes_db');
    const collection = database.collection('animes');
    
    logger.info('Llegint el fitxer XML...');
    const xmlData = await parseXMLFile(xmlFilePath);
    
    logger.info('Processant les dades...');
    const animes = processAnimeData(xmlData);
    
    logger.info('Eliminant dades existents...');
    await collection.deleteMany({});
    
    logger.info('Inserint dades a MongoDB...');
    const result = await collection.insertMany(animes);
    
    logger.info(`${result.insertedCount} documents inserits correctament.`);
    logger.info('Dades carregades amb èxit!');
    
  } catch (error) {
    logger.error('Error carregant les dades a MongoDB:', error);
  } finally {
    await client.close();
    logger.info('Connexió a MongoDB tancada');
  }
}

// Executar la funció principal
loadDataToMongoDB();
