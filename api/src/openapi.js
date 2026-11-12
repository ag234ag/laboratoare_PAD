const bookSchema = {
  type: 'object',
  required: ['title', 'author'],
  properties: {
    id: { type: 'string', example: '000000000000000000000001' },
    title: { type: 'string', example: 'Dune' },
    author: { type: 'string', example: 'Frank Herbert' },
    isbn: { type: 'string', example: '9780441172719' },
    createdAt: { type: 'string', format: 'date-time' },
    updatedAt: { type: 'string', format: 'date-time' },
  },
};

const errorSchema = {
  type: 'object',
  properties: { error: { type: 'string', example: 'Book not found' } },
};

const responseContent = {
  'application/json': { schema: { type: 'object' } },
  'application/xml': { schema: { type: 'string' } },
};

const requestContent = {
  'application/json': { schema: { $ref: '#/components/schemas/BookInput' } },
  'application/xml': { schema: { $ref: '#/components/schemas/BookInput' } },
};

const patchContent = {
  'application/json': { schema: { $ref: '#/components/schemas/BookPatch' } },
  'application/xml': { schema: { $ref: '#/components/schemas/BookPatch' } },
};

const priorityParameter = {
  name: 'X-Priority',
  in: 'header',
  required: false,
  description: 'Prioritatea evenimentului de sincronizare.',
  schema: { type: 'string', enum: ['high', 'normal'], default: 'normal' },
};

const openApiDocument = {
  openapi: '3.0.3',
  info: {
    title: 'Books API',
    version: '1.0.0',
    description: 'API CRUD pentru cărți. Cererile trec prin Nginx Gateway.',
  },
  servers: [{ url: '/', description: 'Nginx API Gateway' }],
  security: [{ ApiKeyAuth: [], DataHouse: [] }],
  tags: [{ name: 'Books', description: 'Operații CRUD asupra cărților' }],
  paths: {
    '/books': {
      get: {
        tags: ['Books'],
        summary: 'Listează cărțile',
        responses: {
          '200': { description: 'Lista cărților', content: responseContent },
          '401': { description: 'Cheie API lipsă sau invalidă', content: { 'application/json': { schema: errorSchema } } },
        },
      },
      post: {
        tags: ['Books'],
        summary: 'Creează o carte',
        parameters: [priorityParameter],
        requestBody: { required: true, content: requestContent },
        responses: {
          '201': { description: 'Cartea a fost creată', content: responseContent },
          '400': { description: 'Date invalide', content: { 'application/json': { schema: errorSchema } } },
          '401': { description: 'Cheie API lipsă sau invalidă', content: { 'application/json': { schema: errorSchema } } },
        },
      },
    },
    '/books/{id}': {
      parameters: [{
        name: 'id', in: 'path', required: true,
        description: 'ID MongoDB format din 24 de caractere hexazecimale',
        schema: { type: 'string', pattern: '^[0-9a-fA-F]{24}$' },
      }],
      get: {
        tags: ['Books'],
        summary: 'Citește o carte',
        responses: {
          '200': { description: 'Cartea găsită', content: responseContent },
          '400': { description: 'ID invalid', content: { 'application/json': { schema: errorSchema } } },
          '404': { description: 'Cartea nu există', content: { 'application/json': { schema: errorSchema } } },
        },
      },
      put: {
        tags: ['Books'],
        summary: 'Creează sau înlocuiește o carte cu ID-ul indicat',
        parameters: [priorityParameter],
        requestBody: { required: true, content: requestContent },
        responses: {
          '200': { description: 'Cartea a fost creată sau înlocuită', content: responseContent },
          '400': { description: 'ID sau date invalide', content: { 'application/json': { schema: errorSchema } } },
        },
      },
      patch: {
        tags: ['Books'],
        summary: 'Modifică parțial o carte existentă',
        parameters: [priorityParameter],
        requestBody: { required: true, content: patchContent },
        responses: {
          '200': { description: 'Cartea a fost modificată', content: responseContent },
          '400': { description: 'ID sau date invalide', content: { 'application/json': { schema: errorSchema } } },
          '404': { description: 'Cartea nu există', content: { 'application/json': { schema: errorSchema } } },
        },
      },
      delete: {
        tags: ['Books'],
        summary: 'Șterge logic o carte și sincronizează modificarea',
        parameters: [priorityParameter],
        responses: {
          '200': { description: 'Cartea a fost ștearsă logic', content: responseContent },
          '400': { description: 'ID invalid', content: { 'application/json': { schema: errorSchema } } },
          '404': { description: 'Cartea nu există', content: { 'application/json': { schema: errorSchema } } },
        },
      },
    },
  },
  components: {
    securitySchemes: {
      ApiKeyAuth: {
        type: 'apiKey',
        in: 'header',
        name: 'X-API-Key',
        description: 'Cheia transmisă la instalarea chart-ului Helm.',
      },
      DataHouse: {
        type: 'apiKey',
        in: 'header',
        name: 'X-Data-House',
        description: 'ID-ul nodului de date, de exemplu house-1 sau house-2.',
      },
    },
    schemas: {
      Book: bookSchema,
      BookInput: {
        type: 'object',
        required: ['title', 'author'],
        properties: {
          title: { type: 'string', example: 'Dune' },
          author: { type: 'string', example: 'Frank Herbert' },
          isbn: { type: 'string', example: '9780441172719' },
        },
        xml: { name: 'book' },
      },
      BookPatch: {
        type: 'object',
        properties: {
          title: { type: 'string', example: 'Dune Messiah' },
          author: { type: 'string', example: 'Frank Herbert' },
          isbn: { type: 'string', example: '9780441172695' },
        },
        xml: { name: 'book' },
      },
      Error: errorSchema,
    },
  },
};

export default openApiDocument;
