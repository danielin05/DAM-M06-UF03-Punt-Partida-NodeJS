const { MongoClient } = require('mongodb');
const fs = require('fs');
const path = require('path');
const winston = require('winston');
const PDFDocument = require('pdfkit');
require('dotenv').config();

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
    new winston.transports.File({ filename: '../../data/logs/exercici2.log' })
  ]
});

// Ruta de sortida dels informes
const outputDir = path.join(__dirname, '../../data/out');
if (!fs.existsSync(outputDir)) {
  fs.mkdirSync(outputDir, { recursive: true });
}

// Funció per generar un informe PDF
async function generarPDF(titol, titolsPreguntes, rutaSortida) {
  const doc = new PDFDocument();
  doc.pipe(fs.createWriteStream(rutaSortida));

  doc.fontSize(18).text(titol, { underline: true });
  doc.moveDown();

  titolsPreguntes.forEach((title, index) => {
    doc.fontSize(12).text(`${index + 1}. ${title}`);
  });

  doc.end();
}

// Funció principal
async function executarConsultes() {
  const uri = process.env.MONGODB_URI || 'mongodb://root:password@127.0.0.1:27017/';
  const client = new MongoClient(uri);

  try {
    await client.connect();
    logger.info('Connectat a MongoDB');

    const db = client.db('animes_db'); // Usa la mateixa BD que a exercici1
    const collection = db.collection('animes'); // I la col·lecció 'questions'

    // 🔹 CONSULTA 1: Preguntes amb ViewCount > mitjana
    const resultatMitjana = await collection.aggregate([
      { $group: { _id: null, mitjanaViewCount: { $avg: "$question.ViewCount"}}}
    ]).toArray();
    
    const mitjanaViewCount = resultatMitjana[0]?.mitjanaViewCount || 0;
    logger.info(`Mitjana de ViewCount: ${mitjanaViewCount.toFixed(2)}`);

    const preguntesSuperiorsMitjana = await collection.find({
      "question.ViewCount": { $gt: mitjanaViewCount }
    }).toArray();

    logger.info(`Preguntes amb ViewCount > mitjana: ${preguntesSuperiorsMitjana.length}`);

    await generarPDF(
      'Informe 1 - Preguntes amb més ViewCount que la mitjana',
      preguntesSuperiorsMitjana.map(doc => doc.question.Title),
      path.join(outputDir, 'informe1.pdf')
    );

    // 🔹 CONSULTA 2: Títols amb paraules clau
    const paraulesClau = ["pug", "wig", "yak", "nap", "jig", "mug", "zap", "gag", "oaf", "elf"];
    const regex = new RegExp(paraulesClau.join('|'), 'i');

    const preguntesParaulesClau = await collection.find({
      "question.Title": { $regex: regex }
    }).toArray();

    logger.info(`Preguntes amb paraules específiques al títol: ${preguntesParaulesClau.length}`);

    await generarPDF(
      'Informe 2 - Preguntes amb paraules específiques al títol',
      preguntesParaulesClau.map(doc => doc.question.Title),
      path.join(outputDir, 'informe2.pdf')
    );

    logger.info('Informes generats correctament a ./data/out/');

  } catch (error) {
    logger.error('Error durant l\'execució:', error);
  } finally {
    await client.close();
    logger.info('Connexió a MongoDB tancada');
  }
}

// Executar
executarConsultes();
