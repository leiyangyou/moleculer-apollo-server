/*
 * moleculer-apollo-server
 * Copyright (c) 2019 MoleculerJS (https://github.com/moleculerjs/moleculer-apollo-server)
 * MIT Licensed
 */

"use strict";

const _ = require("lodash");
const { MoleculerServerError } = require("moleculer").Errors;
const { ApolloServer } = require("./ApolloServer");
const DataLoader = require("dataloader");
const { makeExecutableSchema } = require("graphql-tools");
const GraphQL = require("graphql");
const { PubSub, withFilter } = require("graphql-subscriptions");

module.exports = function(mixinOptions) {
	mixinOptions = _.defaultsDeep(mixinOptions, {
		routeOptions: {
			path: "/graphql",
		},
		schema: null,
		serverOptions: {},
		pubsubFactory() {
			return new PubSub();
		},
		onlyIncludeCurrentService: false,
		createAction: true,
		subscriptionEventName: "graphql.publish",
	});

	const serviceSchema = {
		events: {
			"$services.changed"() {
				this.invalidateGraphQLSchema();
			},
			[mixinOptions.subscriptionEventName](event) {
				if (this.pubsub) {
					this.pubsub.publish(event.tag, event.payload);
				}
			},
		},

		methods: {
			/**
			 * Invalidate the generated GraphQL schema
			 */
			invalidateGraphQLSchema() {
				this.shouldUpdateGraphqlSchema = true;
			},

			/**
			 * Return the field name in a GraphQL Mutation, Query, or Subscription declaration
			 * @param {String} declaration - Mutation, Query, or Subscription declaration
			 * @returns {String} Field name of declaration
			 */
			getFieldName(declaration) {
				// Remove all multi-line/single-line descriptions and comments
				const cleanedDeclaration = declaration
					.replace(/"([\s\S]*?)"/g, "")
					.replace(/^[\s]*?#.*\n?/gm, "")
					.trim();
				return cleanedDeclaration.split(/[(:]/g)[0];
			},

			/**
			 * Get the full name of a service including version spec.
			 *
			 * @param {Service} service - Service object
			 * @returns {String} Name of service including version spec
			 */
			getServiceName(service) {
				if (service.fullName) return service.fullName;

				if (service.version != null)
					return (
						(typeof service.version == "number"
							? "v" + service.version
							: service.version) +
						"." +
						service.name
					);

				return service.name;
			},

			/**
			 * Get action name for resolver
			 *
			 * @param {String} service
			 * @param {String} action
			 */
			getResolverActionName(service, action) {
				if (action.indexOf(".") === -1) {
					return `${service}.${action}`;
				} else {
					return action;
				}
			},

			/**
			 * Create resolvers from service settings
			 *
			 * @param {String} serviceName
			 * @param {Object} resolvers
			 */
			createServiceResolvers(serviceName, resolvers) {
				return Object.entries(resolvers).reduce((acc, [name, r]) => {
					if (_.isPlainObject(r) && r.action != null) {
						// matches signature for remote action resolver
						acc[name] = this.createActionResolver(
							this.getResolverActionName(serviceName, r.action),
							r
						);
					} else {
						// something else (enum, etc.)
						acc[name] = r;
					}

					return acc;
				}, {});
			},

			/**
			 * Create resolver for action
			 *
			 * @param {String} actionName
			 * @param {Object?} def
			 */
			createActionResolver(actionName, def = {}) {
				const {
					dataLoader = false,
					nullIfError = false,
					params = {},
					argParams = {},
					rootParams = {},
					metaParams = {},
				} = def;
				const metaKeys = Object.keys(metaParams);
				const rootKeys = Object.keys(rootParams);
				const argKeys = Object.keys(argParams);

				const firstRootKey = rootKeys[0];

				return async (root, args, context) => {
					const meta = context.ctx.meta;

					try {
						if (dataLoader) {
							let value;

							if (firstRootKey) {
								value = root && _.get(root, firstRootKey);
							}

							if (value == null) {
								return null;
							}

							return Array.isArray(value)
								? await Promise.all(
										value.map(item => context.loaders[actionName].load(item))
								  )
								: await context.loaders[actionName].load(value);
						} else {
							const p = {};

							if (meta && metaKeys.length > 0) {
								metaKeys.forEach(k => _.set(p, metaParams[k], _.get(meta, k)));
							}

							if (root && rootKeys.length > 0) {
								rootKeys.forEach(k => _.set(p, rootParams[k], _.get(root, k)));
							}

							if (argKeys.length > 0) {
								argKeys.forEach(k => {
									_.set(p, argParams[k], _.get(args, k));
									_.unset(args, k);
								});
							}

							return await context.ctx.call(
								actionName,
								_.defaultsDeep(args, p, params)
							);
						}
					} catch (err) {
						if (nullIfError) {
							return null;
						}
						/* istanbul ignore next */
						if (err && err.ctx) {
							err.ctx = null; // Avoid circular JSON in Moleculer <= 0.13
						}
						throw err;
					}
				};
			},

			/**
			 * Create resolver for subscription
			 *
			 * @param {String} actionName
			 * @param {Array?} tags
			 * @param {String?} filter
			 */
			createAsyncIteratorResolver(actionName, tags = [], filter) {
				return {
					subscribe: filter
						? withFilter(
								() => this.pubsub.asyncIterator(tags),
								async (payload, params, ctx) =>
									payload !== undefined
										? this.broker.call(filter, { ...params, payload }, ctx)
										: false
						  )
						: () => this.pubsub.asyncIterator(tags),
					resolve: async (payload, params, ctx) =>
						this.broker.call(actionName, { ...params, payload }, ctx),
				};
			},

			/**
			 * Generate GraphQL Schema
			 *
			 * @param {Object[]} services
			 * @returns {Object} Generated schema
			 */
			generateGraphQLSchema(services) {
				try {
					let typeDefs = [];
					let resolvers = {};
					let schemaDirectives = null;

					if (mixinOptions.typeDefs) {
						typeDefs = typeDefs.concat(mixinOptions.typeDefs);
					}

					if (mixinOptions.resolvers) {
						resolvers = _.cloneDeep(mixinOptions.resolvers);
					}

					if (mixinOptions.schemaDirectives) {
						schemaDirectives = _.cloneDeep(mixinOptions.schemaDirectives);
					}

					let queries = [];
					let mutations = [];
					let subscriptions = [];
					let types = [];
					let interfaces = [];
					let unions = [];
					let enums = [];
					let inputs = [];

					const processedServices = new Set();

					services.forEach(service => {
						const serviceName = this.getServiceName(service);

						// Skip multiple instances of services
						if (processedServices.has(serviceName)) return;
						processedServices.add(serviceName);

						if (service.settings.graphql) {
							// --- COMPILE SERVICE-LEVEL DEFINITIONS ---
							if (_.isObject(service.settings.graphql)) {
								const globalDef = service.settings.graphql;

								if (globalDef.query) {
									queries = queries.concat(globalDef.query);
								}

								if (globalDef.mutation) {
									mutations = mutations.concat(globalDef.mutation);
								}

								if (globalDef.subscription) {
									subscriptions = subscriptions.concat(globalDef.subscription);
								}

								if (globalDef.type) {
									types = types.concat(globalDef.type);
								}

								if (globalDef.interface) {
									interfaces = interfaces.concat(globalDef.interface);
								}

								if (globalDef.union) {
									unions = unions.concat(globalDef.union);
								}

								if (globalDef.enum) {
									enums = enums.concat(globalDef.enum);
								}

								if (globalDef.input) {
									inputs = inputs.concat(globalDef.input);
								}

								if (globalDef.resolvers) {
									resolvers = Object.entries(globalDef.resolvers).reduce(
										(acc, [name, resolver]) => {
											acc[name] = _.merge(
												acc[name] || {},
												this.createServiceResolvers(serviceName, resolver)
											);
											return acc;
										},
										resolvers
									);
								}
							}
						}

						// --- COMPILE ACTION-LEVEL DEFINITIONS ---
						const resolver = {};

						Object.values(service.actions).forEach(action => {
							const { graphql: def } = action;
							if (def && _.isObject(def)) {
								if (def.query) {
									if (!resolver["Query"]) resolver.Query = {};

									_.castArray(def.query).forEach(query => {
										const name = this.getFieldName(query);
										queries.push(query);
										resolver.Query[name] = this.createActionResolver(
											action.name
										);
									});
								}

								if (def.mutation) {
									if (!resolver["Mutation"]) resolver.Mutation = {};

									_.castArray(def.mutation).forEach(mutation => {
										const name = this.getFieldName(mutation);
										mutations.push(mutation);
										resolver.Mutation[name] = this.createActionResolver(
											action.name
										);
									});
								}

								if (def.subscription) {
									if (!resolver["Subscription"]) resolver.Subscription = {};

									_.castArray(def.subscription).forEach(subscription => {
										const name = this.getFieldName(subscription);
										subscriptions.push(subscription);
										resolver.Subscription[
											name
										] = this.createAsyncIteratorResolver(
											action.name,
											def.tags,
											def.filter
										);
									});
								}

								if (def.type) {
									types = types.concat(def.type);
								}

								if (def.interface) {
									interfaces = interfaces.concat(def.interface);
								}

								if (def.union) {
									unions = unions.concat(def.union);
								}

								if (def.enum) {
									enums = enums.concat(def.enum);
								}

								if (def.input) {
									inputs = inputs.concat(def.input);
								}
							}
						});

						if (Object.keys(resolver).length > 0) {
							resolvers = _.merge(resolvers, resolver);
						}
					});

					if (
						queries.length > 0 ||
						types.length > 0 ||
						mutations.length > 0 ||
						subscriptions.length > 0 ||
						interfaces.length > 0 ||
						unions.length > 0 ||
						enums.length > 0 ||
						inputs.length > 0
					) {
						let str = "";
						if (queries.length > 0) {
							str += `
								type Query {
									${queries.join("\n")}
								}
							`;
						}

						if (mutations.length > 0) {
							str += `
								type Mutation {
									${mutations.join("\n")}
								}
							`;
						}

						if (subscriptions.length > 0) {
							str += `
								type Subscription {
									${subscriptions.join("\n")}
								}
							`;
						}

						if (types.length > 0) {
							str += `
								${types.join("\n")}
							`;
						}

						if (interfaces.length > 0) {
							str += `
								${interfaces.join("\n")}
							`;
						}

						if (unions.length > 0) {
							str += `
								${unions.join("\n")}
							`;
						}

						if (enums.length > 0) {
							str += `
								${enums.join("\n")}
							`;
						}

						if (inputs.length > 0) {
							str += `
								${inputs.join("\n")}
							`;
						}

						typeDefs.push(str);
					}

					return makeExecutableSchema({ typeDefs, resolvers, schemaDirectives });
				} catch (err) {
					throw new MoleculerServerError(
						"Unable to compile GraphQL schema",
						500,
						"UNABLE_COMPILE_GRAPHQL_SCHEMA",
						{ err }
					);
				}
			},

			async prepareGraphqlSchema() {
				if (!this._prepareGraphqlSchemaPromise) {
					this._prepareGraphqlSchemaPromise = this.doPrepareGraphqlSchema().then(
						result => {
							this._prepareGraphqlSchemaPromise = null;
							return result;
						},
						err => {
							this._prepareGraphqlSchemaPromise = null;
							throw err;
						}
					);
				}

				return this._prepareGraphqlSchemaPromise;
			},

			async doPrepareGraphqlSchema() {
				// Schema is up-to-date
				if (!this.shouldUpdateGraphqlSchema && this.graphqlHandler) {
					return;
				}

				// Create new server & regenerate GraphQL schema
				this.logger.info(
					"♻ Recreate Apollo GraphQL server and regenerate GraphQL schema..."
				);

				if (this.apolloServer) {
					await this.apolloServer.stop();
				}

				try {
					this.pubsub = mixinOptions.pubsubFactory();
					let services = this.broker.registry.getServiceList({ withActions: true });
					if (mixinOptions.onlyIncludeCurrentService) {
						services = services.filter((s) => s.name === this.name)
					}
					const schema = this.generateGraphQLSchema(services);

					this.logger.debug(
						"Generated GraphQL schema:\n\n" + GraphQL.printSchema(schema)
					);

					this.apolloServer = new ApolloServer({
						schema,
						..._.defaultsDeep({}, mixinOptions.serverOptions, {
							context: ({ req, connection }) => {
								const ctx = req
									? req.$ctx
									: this.broker.ContextFactory.create(this.broker);
								const service = req ? req.$service : this;
								const params = req ? req.$params : {};
								const connectionParams = req
									? req.headers
									: connection.context.connectionParams;
								return {
									ctx,
									service,
									params,
									connectionParams,
									loaders: this.createLoaders(ctx, services),
								};
							},
							subscriptions: {
								onConnect(connectionParams) {
									return {
										connectionParams,
									};
								},
							},
						}),
					});

					this.graphqlHandler = this.apolloServer.createHandler(
						mixinOptions.serverOptions
					);
					this.apolloServer.installSubscriptionHandlers(this.server);
					this.graphqlSchema = schema;

					this.shouldUpdateGraphqlSchema = false;

					this.broker.broadcast("graphql.schema.updated", {
						schema: GraphQL.printSchema(schema),
					});
				} catch (err) {
					this.logger.error(err);
					throw err;
				}
			},

			/**
			 * Create the DataLoader instances to be used for batch resolution
			 * @param {Object} ctx
			 * @param {Object[]} services
			 * @returns {Object.<string, Object>} Key/value pairs of DataLoader instances
			 */
			createLoaders(ctx, services) {
				return services.reduce((serviceAccum, service) => {
					const serviceName = this.getServiceName(service);

					const { graphql } = service.settings;
					if (graphql && graphql.resolvers) {
						const { resolvers } = graphql;

						const typeLoaders = Object.values(resolvers).reduce(
							(resolverAccum, type) => {
								const resolverLoaders = Object.values(type).reduce(
									(fieldAccum, resolver) => {
										if (_.isPlainObject(resolver)) {
											const {
												action,
												dataLoader = false,
												params = {},
												rootParams = {},
												metaParams = {},
											} = resolver;
											const actionParam = Object.values(rootParams)[0]; // use the first root parameter
											if (dataLoader && actionParam) {
												const resolverActionName = this.getResolverActionName(
													serviceName,
													action
												);
												if (fieldAccum[resolverActionName] == null) {
													// create a new DataLoader instance
													fieldAccum[resolverActionName] = new DataLoader(
														keys =>
															ctx.call(
																resolverActionName,
																_.defaultsDeep(
																	{
																		[actionParam]: keys,
																	},
																	params,
																	_.reduce(
																		metaParams,
																		(acc, v, k) => {
																			_.set(
																				acc,
																				v,
																				_.get(ctx.meta, k)
																			);
																			return acc;
																		},
																		{}
																	)
																)
															)
													);
												}
											}
										}
										return fieldAccum;
									},
									{}
								);

								return { ...resolverAccum, ...resolverLoaders };
							},
							{}
						);

						serviceAccum = { ...serviceAccum, ...typeLoaders };
					}

					return serviceAccum;
				}, {});
			},
		},

		created() {
			this.apolloServer = null;
			this.graphqlHandler = null;
			this.graphqlSchema = null;
			this.pubsub = null;
			this.shouldUpdateGraphqlSchema = true;

			const route = _.defaultsDeep(mixinOptions.routeOptions, {
				aliases: {
					async "/"(req, res) {
						try {
							await this.prepareGraphQLSchema();
							return this.graphqlHandler(req, res);
						} catch (err) {
							this.sendError(req, res, err);
						}
					},
					async "/.well-known/apollo/server-health"(req, res) {
						try {
							await this.prepareGraphQLSchema();
						} catch (err) {
							res.statusCode = 503;
							return this.sendResponse(
								req,
								res,
								{ status: "fail", schema: false },
								{ responseType: "application/health+json" }
							);
						}
						return this.graphqlHandler(req, res);
					},
				},

				mappingPolicy: "restrict",

				bodyParsers: {
					json: true,
					urlencoded: { extended: true },
				},
			});

			// Add route
			this.settings.routes.unshift(route);
		},

		started() {
			this.logger.info(`🚀 GraphQL server is available at ${mixinOptions.routeOptions.path}`);
		},
		async stopped() {
			if (this.apolloServer) {
				await this.apolloServer.stop();
			}
		},
	};

	if (mixinOptions.createAction) {
		serviceSchema.actions = {
			graphql: {
				params: {
					query: { type: "string" },
					variables: { type: "object", optional: true },
				},
				async handler(ctx) {
					await this.prepareGraphQLSchema();
					return GraphQL.graphql(
						this.graphqlSchema,
						ctx.params.query,
						null,
						{ ctx },
						ctx.params.variables
					);
				},
			},
		};
	}

	return serviceSchema;
};
