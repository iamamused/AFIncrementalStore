// AFIncrementalStore.m
//
// Copyright (c) 2012 Mattt Thompson (http://mattt.me)
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

#import "AFIncrementalStore.h"
#import "AFHTTPClient.h"
#import <objc/runtime.h>

NSString * const AFIncrementalStoreUnimplementedMethodException = @"com.alamofire.incremental-store.exceptions.unimplemented-method";

NSString * const AFIncrementalStoreContextWillFetchRemoteValues = @"AFIncrementalStoreContextWillFetchRemoteValues";
NSString * const AFIncrementalStoreContextWillSaveRemoteValues = @"AFIncrementalStoreContextWillSaveRemoteValues";
NSString * const AFIncrementalStoreContextDidFetchRemoteValues = @"AFIncrementalStoreContextDidFetchRemoteValues";
NSString * const AFIncrementalStoreContextDidSaveRemoteValues = @"AFIncrementalStoreContextDidSaveRemoteValues";
NSString * const AFIncrementalStoreContextWillFetchNewValuesForObject = @"AFIncrementalStoreContextWillFetchNewValuesForObject";
NSString * const AFIncrementalStoreContextDidFetchNewValuesForObject = @"AFIncrementalStoreContextDidFetchNewValuesForObject";
NSString * const AFIncrementalStoreContextWillFetchNewValuesForRelationship = @"AFIncrementalStoreContextWillFetchNewValuesForRelationship";
NSString * const AFIncrementalStoreContextDidFetchNewValuesForRelationship = @"AFIncrementalStoreContextDidFetchNewValuesForRelationship";

NSString * const AFIncrementalStoreRequestOperationsKey = @"AFIncrementalStoreRequestOperations";
NSString * const AFIncrementalStoreFetchedObjectIDsKey = @"AFIncrementalStoreFetchedObjectIDs";
NSString * const AFIncrementalStoreFaultingObjectIDKey = @"AFIncrementalStoreFaultingObjectID";
NSString * const AFIncrementalStoreFaultingRelationshipKey = @"AFIncrementalStoreFaultingRelationship";
NSString * const AFIncrementalStorePersistentStoreRequestKey = @"AFIncrementalStorePersistentStoreRequest";

static char kAFResourceIdentifierObjectKey;

static NSString * const kAFIncrementalStoreResourceIdentifierAttributeName = @"__af_resourceIdentifier";
static NSString * const kAFIncrementalStoreLastModifiedAttributeName = @"__af_lastModified";
static NSString * const kAFIncrementalStoreEtagAttributeName = @"__af_etag";

static NSString * const kAFReferenceObjectPrefix = @"__af_";

inline NSString * AFReferenceObjectFromResourceIdentifier(NSString *resourceIdentifier) {
    if (!resourceIdentifier) {
        return nil;
    }
    
    return [kAFReferenceObjectPrefix stringByAppendingString:resourceIdentifier];    
}

inline NSString * AFResourceIdentifierFromReferenceObject(id referenceObject) {
    if (!referenceObject) {
        return nil;
    }
    
    NSString *string = [referenceObject description];
    return [string hasPrefix:kAFReferenceObjectPrefix] ? [string substringFromIndex:[kAFReferenceObjectPrefix length]] : string;
}

static inline void AFSaveManagedObjectContextOrThrowInternalConsistencyException(NSManagedObjectContext *managedObjectContext) {
    NSError *error = nil;
    if (![managedObjectContext save:&error]) {
        @throw [NSException exceptionWithName:NSInternalInconsistencyException reason:[error localizedFailureReason] userInfo:[NSDictionary dictionaryWithObject:error forKey:NSUnderlyingErrorKey]];
    }
}

@interface NSManagedObject (_AFIncrementalStore)
@property (readwrite, nonatomic, copy, setter = af_setResourceIdentifier:) NSString *af_resourceIdentifier;
@end

@implementation NSManagedObject (_AFIncrementalStore)
@dynamic af_resourceIdentifier;

- (NSString *)af_resourceIdentifier {
    NSString *identifier = (NSString *)objc_getAssociatedObject(self, &kAFResourceIdentifierObjectKey);
    
    if (!identifier) {
        if ([self.objectID.persistentStore isKindOfClass:[AFIncrementalStore class]]) {
            id referenceObject = [(AFIncrementalStore *)self.objectID.persistentStore referenceObjectForObjectID:self.objectID];
            if ([referenceObject isKindOfClass:[NSString class]]) {
                return AFResourceIdentifierFromReferenceObject(referenceObject);
            }
        }
    }
    
    return identifier;
}

- (void)af_setResourceIdentifier:(NSString *)resourceIdentifier {
    objc_setAssociatedObject(self, &kAFResourceIdentifierObjectKey, resourceIdentifier, OBJC_ASSOCIATION_COPY_NONATOMIC);
}

@end

#pragma mark -

@interface AFIncrementalStore ()
@property (strong, nonatomic) dispatch_queue_t isolationQueue;
@end

@implementation AFIncrementalStore {
@private
	struct
	{
		unsigned int respondsToRequestForInserted:1;
		unsigned int respondsToRequestForUpdated:1;
		unsigned int respondsToRequestForDeleted:1;
		unsigned int respondsToRequestForFetchRequest:1;
		unsigned int respondsToShouldFetchRemoteAttribute:1;
		unsigned int respondsToShouldFetchRemoteRelationship:1;
		
	} _clientFlags;

	
    NSCache *_backingObjectIDByObjectID;
    NSMutableDictionary *_registeredObjectIDsByEntityNameAndNestedResourceIdentifier;
    NSPersistentStoreCoordinator *_backingPersistentStoreCoordinator;
    NSManagedObjectContext *_backingManagedObjectContext;
	NSMutableSet *_expiredObjectIdentifiers;
}
@synthesize HTTPClient = _HTTPClient;
@synthesize backingPersistentStoreCoordinator = _backingPersistentStoreCoordinator;

+ (NSString *)type {
    @throw([NSException exceptionWithName:AFIncrementalStoreUnimplementedMethodException reason:NSLocalizedString(@"Unimplemented method: +type. Must be overridden in a subclass", nil) userInfo:nil]);
}

+ (NSManagedObjectModel *)model {
    @throw([NSException exceptionWithName:AFIncrementalStoreUnimplementedMethodException reason:NSLocalizedString(@"Unimplemented method: +model. Must be overridden in a subclass", nil) userInfo:nil]);
}

#pragma mark -

- (void)setHTTPClient:(AFHTTPClient<AFIncrementalStoreHTTPClient> *)HTTPClient
{
	_HTTPClient = HTTPClient;
	
	_clientFlags.respondsToRequestForDeleted = [_HTTPClient respondsToSelector:@selector(requestForDeletedObject:)];
	_clientFlags.respondsToRequestForUpdated = [_HTTPClient respondsToSelector:@selector(requestForUpdatedObject:)];
	_clientFlags.respondsToRequestForInserted = [_HTTPClient respondsToSelector:@selector(requestForInsertedObject:)];
	_clientFlags.respondsToShouldFetchRemoteAttribute = [_HTTPClient respondsToSelector:@selector(shouldFetchRemoteAttributeValuesForObjectWithID:inManagedObjectContext:)];
	_clientFlags.respondsToShouldFetchRemoteRelationship = [_HTTPClient respondsToSelector:@selector(shouldFetchRemoteValuesForRelationship:forObjectWithID:inManagedObjectContext:)];
}

- (void)notifyManagedObjectContext:(NSManagedObjectContext *)context
             aboutRequestOperation:(AFHTTPRequestOperation *)operation
                   forFetchRequest:(NSFetchRequest *)fetchRequest
                  fetchedObjectIDs:(NSArray *)fetchedObjectIDs
{
    NSString *notificationName = [operation isFinished] ? AFIncrementalStoreContextDidFetchRemoteValues : AFIncrementalStoreContextWillFetchRemoteValues;
    
    NSMutableDictionary *userInfo = [NSMutableDictionary dictionary];
    [userInfo setObject:[NSArray arrayWithObject:operation] forKey:AFIncrementalStoreRequestOperationsKey];
    [userInfo setObject:fetchRequest forKey:AFIncrementalStorePersistentStoreRequestKey];
    if ([operation isFinished] && fetchedObjectIDs) {
        [userInfo setObject:fetchedObjectIDs forKey:AFIncrementalStoreFetchedObjectIDsKey];
    }
 
	dispatch_async(dispatch_get_main_queue(), ^{
		[[NSNotificationCenter defaultCenter] postNotificationName:notificationName object:context userInfo:userInfo];
	});
}

- (void)notifyManagedObjectContext:(NSManagedObjectContext *)context
            aboutRequestOperations:(NSArray *)operations
             forSaveChangesRequest:(NSSaveChangesRequest *)saveChangesRequest
{
    NSString *notificationName = [[operations lastObject] isFinished] ? AFIncrementalStoreContextDidSaveRemoteValues : AFIncrementalStoreContextWillSaveRemoteValues;
    
    NSMutableDictionary *userInfo = [NSMutableDictionary dictionary];
    [userInfo setObject:operations forKey:AFIncrementalStoreRequestOperationsKey];
    [userInfo setObject:saveChangesRequest forKey:AFIncrementalStorePersistentStoreRequestKey];

	dispatch_async(dispatch_get_main_queue(), ^{
		[[NSNotificationCenter defaultCenter] postNotificationName:notificationName object:context userInfo:userInfo];
	});
}

- (void)notifyManagedObjectContext:(NSManagedObjectContext *)context
             aboutRequestOperation:(AFHTTPRequestOperation *)operation
       forNewValuesForObjectWithID:(NSManagedObjectID *)objectID
{
    NSString *notificationName = [operation isFinished] ? AFIncrementalStoreContextDidFetchNewValuesForObject :AFIncrementalStoreContextWillFetchNewValuesForObject;

    NSMutableDictionary *userInfo = [NSMutableDictionary dictionary];
    [userInfo setObject:[NSArray arrayWithObject:operation] forKey:AFIncrementalStoreRequestOperationsKey];
    [userInfo setObject:objectID forKey:AFIncrementalStoreFaultingObjectIDKey];

	dispatch_async(dispatch_get_main_queue(), ^{
		[[NSNotificationCenter defaultCenter] postNotificationName:notificationName object:context userInfo:userInfo];
	});
}

- (void)notifyManagedObjectContext:(NSManagedObjectContext *)context
             aboutRequestOperation:(AFHTTPRequestOperation *)operation
       forNewValuesForRelationship:(NSRelationshipDescription *)relationship
                   forObjectWithID:(NSManagedObjectID *)objectID
{
    NSString *notificationName = [operation isFinished] ? AFIncrementalStoreContextDidFetchNewValuesForRelationship : AFIncrementalStoreContextWillFetchNewValuesForRelationship;

    NSMutableDictionary *userInfo = [NSMutableDictionary dictionary];
    [userInfo setObject:[NSArray arrayWithObject:operation] forKey:AFIncrementalStoreRequestOperationsKey];
    [userInfo setObject:objectID forKey:AFIncrementalStoreFaultingObjectIDKey];
    [userInfo setObject:relationship forKey:AFIncrementalStoreFaultingRelationshipKey];

	dispatch_async(dispatch_get_main_queue(), ^{
		[[NSNotificationCenter defaultCenter] postNotificationName:notificationName object:context userInfo:userInfo];
	});
}

#pragma mark -

- (NSManagedObjectContext *)backingManagedObjectContext {
    if (!_backingManagedObjectContext) {
        _backingManagedObjectContext = [[NSManagedObjectContext alloc] initWithConcurrencyType:NSPrivateQueueConcurrencyType];
        _backingManagedObjectContext.persistentStoreCoordinator = _backingPersistentStoreCoordinator;
        _backingManagedObjectContext.retainsRegisteredObjects = YES;
    }
    
    return _backingManagedObjectContext;
}

- (NSManagedObjectID *)objectIDForEntity:(NSEntityDescription *)entity
                  withResourceIdentifier:(NSString *)resourceIdentifier
{
    if (!resourceIdentifier) {
        return nil;
    }
    
    __block NSManagedObjectID *objectID = nil;
	__block NSMutableDictionary *objectIDsByResourceIdentifier;

	dispatch_sync(self.isolationQueue, ^{
		objectIDsByResourceIdentifier = [_registeredObjectIDsByEntityNameAndNestedResourceIdentifier objectForKey:entity.name];
	});
	
	if (objectIDsByResourceIdentifier) {
		objectID = [objectIDsByResourceIdentifier objectForKey:resourceIdentifier];
	}
    
    if (!objectID) {
        objectID = [self newObjectIDForEntity:entity referenceObject:AFReferenceObjectFromResourceIdentifier(resourceIdentifier)];
    }
    
    NSParameterAssert([objectID.entity.name isEqualToString:entity.name]);
    
    return objectID;
}

- (NSManagedObjectID *)objectIDForBackingObjectForEntity:(NSEntityDescription *)entity
                                  withResourceIdentifier:(NSString *)resourceIdentifier
{
    if (!resourceIdentifier) {
        return nil;
    }

    NSManagedObjectID *objectID = [self objectIDForEntity:entity withResourceIdentifier:resourceIdentifier];
    __block NSManagedObjectID *backingObjectID = [_backingObjectIDByObjectID objectForKey:objectID];
    if (backingObjectID) {
        return backingObjectID;
    }

    NSFetchRequest *fetchRequest = [[NSFetchRequest alloc] initWithEntityName:[entity name]];
    fetchRequest.resultType = NSManagedObjectIDResultType;
    fetchRequest.fetchLimit = 1;
    fetchRequest.predicate = [NSPredicate predicateWithFormat:@"%K = %@", kAFIncrementalStoreResourceIdentifierAttributeName, resourceIdentifier];
    
    __block NSError *error = nil;
    NSManagedObjectContext *backingContext = [self backingManagedObjectContext];
    [backingContext performBlockAndWait:^{
        backingObjectID = [[backingContext executeFetchRequest:fetchRequest error:&error] lastObject];
    }];
    
    if (error) {
        NSLog(@"Error: %@", error);
        return nil;
    }

    if (backingObjectID) {
        [_backingObjectIDByObjectID setObject:backingObjectID forKey:objectID];
    }
    
    return backingObjectID;
}

- (void)updateBackingObject:(NSManagedObject *)backingObject
withValuesFromManagedObject:(NSManagedObject *)managedObject
					context:(NSManagedObjectContext *)context
{
	NSManagedObjectContext *backingContext = [self backingManagedObjectContext];
    NSMutableDictionary *mutableRelationshipValues = [[NSMutableDictionary alloc] init];
	__block NSDictionary *attributeValues = nil;
	
	[context performBlockAndWait:^{
		attributeValues = [managedObject dictionaryWithValuesForKeys:[managedObject.entity.attributesByName allKeys]];
		
		for (NSRelationshipDescription *relationship in [managedObject.entity.relationshipsByName allValues]) {
			
			if ([managedObject hasFaultForRelationshipNamed:relationship.name]) {
				continue;
			}
			
			id relationshipValue = [managedObject valueForKey:relationship.name];
			if (!relationshipValue) {
				continue;
			}
			
			if ([relationship isToMany]) {
				id mutableBackingRelationshipValue = nil;
				if ([relationship isOrdered]) {
					mutableBackingRelationshipValue = [NSMutableOrderedSet orderedSetWithCapacity:[relationshipValue count]];
				} else {
					mutableBackingRelationshipValue = [NSMutableSet setWithCapacity:[relationshipValue count]];
				}
				
				for (NSManagedObject *relationshipManagedObject in relationshipValue) {
					if ([[relationshipManagedObject objectID] isTemporaryID]) {
						continue;
					}
					
					NSString *resourceIdentifier = AFResourceIdentifierFromReferenceObject([self referenceObjectForObjectID:relationshipManagedObject.objectID]);
					NSManagedObjectID *backingRelationshipObjectID = [self objectIDForBackingObjectForEntity:relationship.destinationEntity
																					  withResourceIdentifier:resourceIdentifier];
					if (backingRelationshipObjectID) {
						[backingContext performBlockAndWait:^{
							NSManagedObject *backingRelationshipObject = [backingObject.managedObjectContext existingObjectWithID:backingRelationshipObjectID error:nil];
							if (backingRelationshipObject) {
								[mutableBackingRelationshipValue addObject:backingRelationshipObject];
							}
						}];
					}
				}
				
				[mutableRelationshipValues setValue:mutableBackingRelationshipValue forKey:relationship.name];
			} else {
				if ([[relationshipValue objectID] isTemporaryID]) {
					continue;
				}
				
				NSString *resourceIdentifier = AFResourceIdentifierFromReferenceObject([self referenceObjectForObjectID:[relationshipValue objectID]]);
				NSManagedObjectID *backingRelationshipObjectID = [self objectIDForBackingObjectForEntity:relationship.destinationEntity
																				  withResourceIdentifier:resourceIdentifier];
				if (backingRelationshipObjectID) {
					[backingContext performBlockAndWait:^{
						NSManagedObject *backingRelationshipObject = [backingObject.managedObjectContext existingObjectWithID:backingRelationshipObjectID error:nil];
						[mutableRelationshipValues setValue:backingRelationshipObject forKey:relationship.name];
					}];
				}
			}
		}
	}];

    
	[backingContext performBlockAndWait:^{
		[backingObject setValuesForKeysWithDictionary:mutableRelationshipValues];
		[backingObject setValuesForKeysWithDictionary:attributeValues];
	}];
}

- (NSArray *)unsafeObjectsForObjectIDs:(NSArray *)objectIDs context:(NSManagedObjectContext *)context
{
	NSMutableArray *unsafeObjects = [[NSMutableArray alloc] initWithCapacity:[objectIDs count]];
	[context performBlockAndWait:^{
		for (NSManagedObjectID *objectID in objectIDs) {
			NSManagedObject *managedObject = [context objectWithID:objectID];
			if (managedObject) {
				[unsafeObjects addObject:managedObject];
			}
		}
	}];
	
	return unsafeObjects;
}

// Typically used after an insertOrUpdateObjectsFromRepresentations

- (void)updateRelationship:(NSRelationshipDescription *)relationship
		  forManagedObject:(NSManagedObject *)managedObject
				 inContext:(NSManagedObjectContext *)context
			 backingObject:(NSManagedObject *)backingObject
			 withObjectIDs:(NSArray *)managedObjectIDs
		  backingObjectIDs:(NSArray *)backingObjectIDs
{
	NSManagedObjectContext *backingContext = [self backingManagedObjectContext];
	
	// Don't touch the objects in these arrays outside of a MOC concurrency block
	NSArray *managedObjects = [self unsafeObjectsForObjectIDs:managedObjectIDs context:context];
	NSArray *backingObjects = [self unsafeObjectsForObjectIDs:backingObjectIDs context:backingContext];
	
	// Also off limits outside the MOC blocks!
	__block id managedObjectValue = nil;
	__block	id backingObjectValue = nil;
	
	if ([relationship isToMany]) {
		if ([relationship isOrdered]) {
			managedObjectValue = [NSOrderedSet orderedSetWithArray:managedObjects];
			backingObjectValue = [NSOrderedSet orderedSetWithArray:backingObjects];
		} else {
			managedObjectValue = [NSSet setWithArray:managedObjects];
			backingObjectValue = [NSSet setWithArray:backingObjects];
		}
	} else {
		managedObjectValue = [managedObjects lastObject];
		backingObjectValue = [backingObjects lastObject];
	}
	
	[context performBlockAndWait:^{
		[managedObject setValue:managedObjectValue forKey:relationship.name];
	}];
	
	[backingContext performBlockAndWait:^{
		[backingObject setValue:backingObjectValue forKey:relationship.name];
	}];

}

#pragma mark -

- (BOOL)insertOrUpdateObjectsFromRepresentations:(id)representationOrArrayOfRepresentations
                                        ofEntity:(NSEntityDescription *)entity
                                    fromResponse:(NSHTTPURLResponse *)response
                                     withContext:(NSManagedObjectContext *)context
                                           error:(NSError *__autoreleasing *)error
                                 completionBlock:(void (^)(NSArray *managedObjectsIDs, NSArray *backingObjectIDs))completionBlock
{
    if (!representationOrArrayOfRepresentations) {
        return NO;
    }

    NSParameterAssert([representationOrArrayOfRepresentations isKindOfClass:[NSArray class]] || [representationOrArrayOfRepresentations isKindOfClass:[NSDictionary class]]);
    
    if ([representationOrArrayOfRepresentations count] == 0) {
        if (completionBlock) {
            completionBlock([NSArray array], [NSArray array]);
        }
        
        return NO;
    }
    
    NSManagedObjectContext *backingContext = [self backingManagedObjectContext];
    NSString *lastModified = [[response allHeaderFields] valueForKey:@"Last-Modified"];
	NSString *etag = [[response allHeaderFields] valueForKey:@"Etag"];

    NSArray *representations = nil;
    if ([representationOrArrayOfRepresentations isKindOfClass:[NSArray class]]) {
        representations = representationOrArrayOfRepresentations;
    } else if ([representationOrArrayOfRepresentations isKindOfClass:[NSDictionary class]]) {
        representations = [NSArray arrayWithObject:representationOrArrayOfRepresentations];
    }

    NSUInteger numberOfRepresentations = [representations count];
    NSMutableArray *mutableManagedObjectIDs = [NSMutableArray arrayWithCapacity:numberOfRepresentations];
    NSMutableArray *mutableBackingObjectIDs = [NSMutableArray arrayWithCapacity:numberOfRepresentations];
    
    for (NSDictionary *representation in representations) {
        NSString *resourceIdentifier = [self.HTTPClient resourceIdentifierForRepresentation:representation ofEntity:entity fromResponse:response];
		if (nil == resourceIdentifier) {
			continue;
		}
		
		// Don't touch these, period, unless you are doing so from a performBlock call
		__block NSManagedObject *managedObject = nil;
		__block NSManagedObject *backingObject = nil;
		NSManagedObjectID *objectID = [self objectIDForEntity:entity withResourceIdentifier:resourceIdentifier];
		NSManagedObjectID *backingObjectID = [self objectIDForBackingObjectForEntity:entity withResourceIdentifier:resourceIdentifier];
		if (objectID) {
			[mutableManagedObjectIDs addObject:objectID];
		}
		if (backingObjectID) {
			[mutableBackingObjectIDs addObject:backingObjectID];
		}
		
        NSDictionary *attributes = [self.HTTPClient attributesForRepresentation:representation ofEntity:entity fromResponse:response];

		// Update the backing object's attributes with new values
        [backingContext performBlockAndWait:^{
            if (backingObjectID) {
                backingObject = [backingContext existingObjectWithID:backingObjectID error:nil];
            } else {
                backingObject = [NSEntityDescription insertNewObjectForEntityForName:entity.name inManagedObjectContext:backingContext];
                [backingObject.managedObjectContext obtainPermanentIDsForObjects:[NSArray arrayWithObject:backingObject] error:nil];
            }
			
			[backingObject setValue:resourceIdentifier forKey:kAFIncrementalStoreResourceIdentifierAttributeName];
			[backingObject setValue:lastModified forKey:kAFIncrementalStoreLastModifiedAttributeName];
			[backingObject setValue:etag forKey:kAFIncrementalStoreEtagAttributeName];
			[backingObject setValuesForKeysWithDictionary:attributes];
        }];
		
		// Update the object's attributes from the provided context, inserting it if necessary
        [context performBlockAndWait:^{
			managedObject = [context existingObjectWithID:objectID error:nil];
			[managedObject setValuesForKeysWithDictionary:attributes];
			if (!backingObjectID) {
				[context insertObject:managedObject];
			}
        }];
        
        
        NSDictionary *relationshipRepresentations = [self.HTTPClient representationsForRelationshipsFromRepresentation:representation ofEntity:entity fromResponse:response];
        for (NSString *relationshipName in relationshipRepresentations) {
            NSRelationshipDescription *relationship = [[entity relationshipsByName] valueForKey:relationshipName];
            id relationshipRepresentation = [relationshipRepresentations objectForKey:relationshipName];
            if (!relationship || (relationship.isOptional && (!relationshipRepresentation || [relationshipRepresentation isEqual:[NSNull null]]))) {
                continue;
            }
                        
            if (!relationshipRepresentation || [relationshipRepresentation isEqual:[NSNull null]] || [relationshipRepresentation count] == 0) {
				[context performBlockAndWait:^{
					[managedObject setValue:nil forKey:relationshipName];
				}];
                
				[backingContext performBlockAndWait:^{
					[backingObject setValue:nil forKey:relationshipName];
				}];
                
                continue;
            }
            
            [self insertOrUpdateObjectsFromRepresentations:relationshipRepresentation ofEntity:relationship.destinationEntity fromResponse:response withContext:context error:error completionBlock:^(NSArray *managedObjectIDs, NSArray *backingObjectIDs) {
				
				[self updateRelationship:relationship
						forManagedObject:managedObject
							   inContext:context
						   backingObject:backingObject
						   withObjectIDs:managedObjectIDs
						backingObjectIDs:backingObjectIDs];
            }];
        }
    }
    
    if (completionBlock) {
        completionBlock(mutableManagedObjectIDs, mutableBackingObjectIDs);
    }

    return YES;
}

- (id)executeFetchRequest:(NSFetchRequest *)fetchRequest
              withContext:(NSManagedObjectContext *)context
                    error:(NSError *__autoreleasing *)error
{
    NSManagedObjectContext *backingContext = [self backingManagedObjectContext];
	NSFetchRequest *backingFetchRequest = [fetchRequest copy];
	[backingContext performBlockAndWait:^{
		backingFetchRequest.entity = [NSEntityDescription entityForName:fetchRequest.entityName inManagedObjectContext:backingContext];
	}];
	
	__block id fetchResults = nil;
	
    switch (fetchRequest.resultType) {
        case NSManagedObjectResultType: {
			__block NSMutableArray *mutableObjects = nil;
            backingFetchRequest.resultType = NSDictionaryResultType;
            backingFetchRequest.propertiesToFetch = [NSArray arrayWithObject:kAFIncrementalStoreResourceIdentifierAttributeName];
			[backingContext performBlockAndWait:^{
				NSArray *results = [backingContext executeFetchRequest:backingFetchRequest error:error];
				
				mutableObjects = [NSMutableArray arrayWithCapacity:[results count]];
				NSArray *resourceIdentifiers = [results valueForKeyPath:kAFIncrementalStoreResourceIdentifierAttributeName];
				[context performBlockAndWait:^{
					for (NSString *resourceIdentifier in resourceIdentifiers) {
						NSManagedObjectID *objectID = [self objectIDForEntity:fetchRequest.entity withResourceIdentifier:resourceIdentifier];
						NSManagedObject *object = [context objectWithID:objectID];
						object.af_resourceIdentifier = resourceIdentifier;
						[mutableObjects addObject:object];
					}
				}];
			}];
            
            fetchResults = mutableObjects;
        } break;
        case NSManagedObjectIDResultType: {
			__block NSMutableArray *managedObjectIDs = nil;
			[backingContext performBlockAndWait:^{
				NSArray *backingObjectIDs = [backingContext executeFetchRequest:backingFetchRequest error:error];
				backingObjectIDs = [NSMutableArray arrayWithCapacity:[backingObjectIDs count]];

				for (NSManagedObjectID *backingObjectID in backingObjectIDs) {
					NSManagedObject *backingObject = [backingContext objectWithID:backingObjectID];
					NSString *resourceID = [backingObject valueForKey:kAFIncrementalStoreResourceIdentifierAttributeName];
					[managedObjectIDs addObject:[self objectIDForEntity:fetchRequest.entity withResourceIdentifier:resourceID]];
				}

			}];
            
			fetchResults = managedObjectIDs;
        } break;
        case NSDictionaryResultType:
        case NSCountResultType: {
			[backingContext performBlockAndWait:^{
				fetchResults = [backingContext executeFetchRequest:backingFetchRequest error:error];
			}];
		} break;
        default:
            fetchResults = nil;
    }
	
	// Fetch from the network
	NSURLRequest *request = [self.HTTPClient requestForFetchRequest:fetchRequest withContext:context];
    if (![request URL]) {
		return fetchResults;
    }
	
	AFHTTPRequestOperation *operation = [self.HTTPClient HTTPRequestOperationWithRequest:request success:^(AFHTTPRequestOperation *operation, id responseObject) {
		id representationOrArrayOfRepresentations = [self.HTTPClient representationOrArrayOfRepresentationsOfEntity:fetchRequest.entity fromResponseObject:responseObject];
		
		NSManagedObjectContext *childContext = [[NSManagedObjectContext alloc] initWithConcurrencyType:NSPrivateQueueConcurrencyType];
		childContext.parentContext = context;
		childContext.mergePolicy = NSMergeByPropertyObjectTrumpMergePolicy;
		
		[self insertOrUpdateObjectsFromRepresentations:representationOrArrayOfRepresentations ofEntity:fetchRequest.entity fromResponse:operation.response withContext:childContext error:nil completionBlock:^(NSArray *managedObjectIDs, NSArray *backingObjectIDs) {
			
			__block NSArray *childObjectIDs = nil;
			
			[childContext performBlockAndWait:^{
				NSSet *childObjects = [childContext registeredObjects];
				childObjectIDs = [childObjects valueForKeyPath:@"objectID"];
				AFSaveManagedObjectContextOrThrowInternalConsistencyException(childContext);
			}];
			
			NSManagedObjectContext *backingContext = [self backingManagedObjectContext];
			[backingContext performBlockAndWait:^{
				AFSaveManagedObjectContextOrThrowInternalConsistencyException(backingContext);
			}];
			
			for (NSManagedObjectID *childObjectID in childObjectIDs) {
				NSManagedObject *parentObject = [context objectWithID:childObjectID];
				[context refreshObject:parentObject mergeChanges:NO];
			}
			
			[self notifyManagedObjectContext:context aboutRequestOperation:operation forFetchRequest:fetchRequest fetchedObjectIDs:managedObjectIDs];
		}];
	} failure:^(AFHTTPRequestOperation *operation, NSError *error) {
		NSLog(@"Error: %@", error);
		[self notifyManagedObjectContext:context aboutRequestOperation:operation forFetchRequest:fetchRequest fetchedObjectIDs:nil];
	}];
	
	[self notifyManagedObjectContext:context aboutRequestOperation:operation forFetchRequest:fetchRequest fetchedObjectIDs:nil];
	[self.HTTPClient enqueueHTTPRequestOperation:operation];
	
	return fetchResults;
}

- (void)saveChangesRequestForInsertedObjects:(NSSaveChangesRequest *)saveChangesRequest
								 withContext:(NSManagedObjectContext *)context
						   mutableOperations:(NSMutableArray *)mutableOperations
{
	if (!_clientFlags.respondsToRequestForInserted) {
		return;
    }
	
	NSManagedObjectContext *backingContext = [self backingManagedObjectContext];
	
	for (NSManagedObject *insertedObject in [saveChangesRequest insertedObjects]) {
		NSEntityDescription *entity = [insertedObject entity];
		NSURLRequest *request = [self.HTTPClient requestForInsertedObject:insertedObject];
		if (!request) {
			[backingContext performBlockAndWait:^{
				CFUUIDRef UUID = CFUUIDCreate(NULL);
				NSString *resourceIdentifier = (__bridge_transfer NSString *)CFUUIDCreateString(NULL, UUID);
				CFRelease(UUID);
				
				NSManagedObject *backingObject = [NSEntityDescription insertNewObjectForEntityForName:entity.name inManagedObjectContext:backingContext];
				[backingObject.managedObjectContext obtainPermanentIDsForObjects:[NSArray arrayWithObject:backingObject] error:nil];
				[backingObject setValue:resourceIdentifier forKey:kAFIncrementalStoreResourceIdentifierAttributeName];
				[self updateBackingObject:backingObject withValuesFromManagedObject:insertedObject context:context];
				[backingContext save:nil];
			}];
			
			[insertedObject willChangeValueForKey:@"objectID"];
			[context obtainPermanentIDsForObjects:[NSArray arrayWithObject:insertedObject] error:nil];
			[insertedObject didChangeValueForKey:@"objectID"];
			continue;
		}
		
		AFHTTPRequestOperation *operation = [self.HTTPClient HTTPRequestOperationWithRequest:request success:^(AFHTTPRequestOperation *operation, id responseObject) {
			id representationOrArrayOfRepresentations = [self.HTTPClient representationOrArrayOfRepresentationsOfEntity:entity  fromResponseObject:responseObject];
			if (NO == [representationOrArrayOfRepresentations isKindOfClass:[NSDictionary class]]) {
				return;
			}
			
			NSDictionary *representation = (NSDictionary *)representationOrArrayOfRepresentations;
			NSString *resourceIdentifier = [self.HTTPClient resourceIdentifierForRepresentation:representation
																					   ofEntity:entity
																				   fromResponse:operation.response];
			
			NSManagedObjectID *backingObjectID = [self objectIDForBackingObjectForEntity:entity withResourceIdentifier:resourceIdentifier];
			[context performBlockAndWait:^{
				insertedObject.af_resourceIdentifier = resourceIdentifier;
				NSDictionary *values = [self.HTTPClient attributesForRepresentation:representation ofEntity:insertedObject.entity fromResponse:operation.response];
				[insertedObject setValuesForKeysWithDictionary:values];
			}];
			
			[backingContext performBlockAndWait:^{
				NSManagedObject *backingObject = nil;
				if (backingObjectID) {
					backingObject = [backingContext existingObjectWithID:backingObjectID error:nil];
				}
				
				if (!backingObject) {
					backingObject = [NSEntityDescription insertNewObjectForEntityForName:entity.name inManagedObjectContext:backingContext];
					[backingObject.managedObjectContext obtainPermanentIDsForObjects:[NSArray arrayWithObject:backingObject] error:nil];
				}
				
				[backingObject setValue:resourceIdentifier forKey:kAFIncrementalStoreResourceIdentifierAttributeName];
				[self updateBackingObject:backingObject withValuesFromManagedObject:insertedObject context:context];
				[backingContext save:nil];
			}];
			
			[context performBlockAndWait:^{
				[insertedObject willChangeValueForKey:@"objectID"];
				[context obtainPermanentIDsForObjects:[NSArray arrayWithObject:insertedObject] error:nil];
				[insertedObject didChangeValueForKey:@"objectID"];
				
				[context refreshObject:insertedObject mergeChanges:NO];
			}];

		} failure:^(AFHTTPRequestOperation *operation, NSError *error) {
			NSLog(@"Insert Error: %@", error);
			
			// Reset destination objects to prevent dangling relationships
			for (NSRelationshipDescription *relationship in [entity.relationshipsByName allValues]) {
				if (!relationship.inverseRelationship) {
					continue;
				}
				
				[context performBlockAndWait:^{
					id <NSFastEnumeration> destinationObjects = nil;
					if ([relationship isToMany]) {
						destinationObjects = [insertedObject valueForKey:relationship.name];
					} else {
						NSManagedObject *destinationObject = [insertedObject valueForKey:relationship.name];
						if (destinationObject) {
							destinationObjects = [NSArray arrayWithObject:destinationObject];
						}
					}
					
					for (NSManagedObject *destinationObject in destinationObjects) {
						[context refreshObject:destinationObject mergeChanges:NO];
					}
				}];
			}
		}];
		
		[mutableOperations addObject:operation];
	}
}

- (void)saveChangesRequestForUpdatedObjects:(NSSaveChangesRequest *)saveChangesRequest
								withContext:(NSManagedObjectContext *)context
						  mutableOperations:(NSMutableArray *)mutableOperations
{
	if (!_clientFlags.respondsToRequestForUpdated) {
		return;
    }

	NSManagedObjectContext *backingContext = [self backingManagedObjectContext];
	
	for (NSManagedObject *updatedObject in [saveChangesRequest updatedObjects]) {
		NSEntityDescription *entity = [updatedObject entity];
		NSString *resourceIdentifier = AFResourceIdentifierFromReferenceObject([self referenceObjectForObjectID:updatedObject.objectID]);
		NSManagedObjectID *backingObjectID = [self objectIDForBackingObjectForEntity:entity withResourceIdentifier:resourceIdentifier];
		
		NSURLRequest *request = [self.HTTPClient requestForUpdatedObject:updatedObject];
		if (!request) {
			[backingContext performBlockAndWait:^{
				NSManagedObject *backingObject = [backingContext existingObjectWithID:backingObjectID error:nil];
				[self updateBackingObject:backingObject withValuesFromManagedObject:updatedObject context:context];
				[backingContext save:nil];
			}];
			continue;
		}
		
		AFHTTPRequestOperation *operation = [self.HTTPClient HTTPRequestOperationWithRequest:request success:^(AFHTTPRequestOperation *operation, id responseObject) {
			id representationOrArrayOfRepresentations = [self.HTTPClient representationOrArrayOfRepresentationsOfEntity:entity  fromResponseObject:responseObject];

			if (NO == [representationOrArrayOfRepresentations isKindOfClass:[NSDictionary class]]) {
				return;
			}
			
			[context performBlockAndWait:^{
				NSDictionary *representation = (NSDictionary *)representationOrArrayOfRepresentations;
				NSDictionary *values = [self.HTTPClient attributesForRepresentation:representation ofEntity:updatedObject.entity fromResponse:operation.response];
				[updatedObject setValuesForKeysWithDictionary:values];
			}];
			
			[backingContext performBlockAndWait:^{
				NSManagedObject *backingObject = [backingContext existingObjectWithID:backingObjectID error:nil];
				[self updateBackingObject:backingObject withValuesFromManagedObject:updatedObject context:context];
				[backingContext save:nil];
			}];
			
			[context performBlockAndWait:^{
				[context refreshObject:updatedObject mergeChanges:YES];
			}];

		} failure:^(AFHTTPRequestOperation *operation, NSError *error) {
			NSLog(@"Update Error: %@", error);
			[context performBlockAndWait:^{
				[context refreshObject:updatedObject mergeChanges:NO];
			}];
		}];
		
		[mutableOperations addObject:operation];
	}
}

- (void)saveChangesRequestForDeletedObjects:(NSSaveChangesRequest *)saveChangesRequest
								withContext:(NSManagedObjectContext *)context
						  mutableOperations:(NSMutableArray *)mutableOperations
{
	if (_clientFlags.respondsToRequestForDeleted) {
		return;
    }
	
	NSManagedObjectContext *backingContext = [self backingManagedObjectContext];
	
	for (NSManagedObject *deletedObject in [saveChangesRequest deletedObjects]) {
		// Don't send requests for expired
		__block BOOL isExpired = NO;
		dispatch_barrier_async(self.isolationQueue, ^{
			if ([_expiredObjectIdentifiers containsObject:[deletedObject objectID]]) {
				isExpired = YES;
				[_expiredObjectIdentifiers removeObject:[deletedObject objectID]];
			}
		});
		
		if (isExpired) {
			continue;
		}
		
		NSEntityDescription *entity = [deletedObject entity];
		NSString *resourceIdentifier = AFResourceIdentifierFromReferenceObject([self referenceObjectForObjectID:deletedObject.objectID]);
		NSManagedObjectID *backingObjectID = [self objectIDForBackingObjectForEntity:entity withResourceIdentifier:resourceIdentifier];
		
		NSURLRequest *request = [self.HTTPClient requestForDeletedObject:deletedObject];
		if (!request) {
			[backingContext performBlockAndWait:^{
				NSManagedObject *backingObject = [backingContext existingObjectWithID:backingObjectID error:nil];
				[backingContext deleteObject:backingObject];
				[backingContext save:nil];
			}];
			continue;
		}
		
		AFHTTPRequestOperation *operation = [self.HTTPClient HTTPRequestOperationWithRequest:request success:^(AFHTTPRequestOperation *operation, id responseObject) {
			[backingContext performBlockAndWait:^{
				NSManagedObject *backingObject = [backingContext existingObjectWithID:backingObjectID error:nil];
				[backingContext deleteObject:backingObject];
				[backingContext save:nil];
			}];
		} failure:^(AFHTTPRequestOperation *operation, NSError *error) {
			NSLog(@"Delete Error: %@", error);
		}];
		
		[mutableOperations addObject:operation];
	}
}


- (id)executeSaveChangesRequest:(NSSaveChangesRequest *)saveChangesRequest
                    withContext:(NSManagedObjectContext *)context
                          error:(NSError *__autoreleasing *)error
{
    NSMutableArray *mutableOperations = [NSMutableArray array];
    
	[self saveChangesRequestForInsertedObjects:saveChangesRequest withContext:context mutableOperations:mutableOperations];
	[self saveChangesRequestForUpdatedObjects:saveChangesRequest withContext:context mutableOperations:mutableOperations];
    [self saveChangesRequestForDeletedObjects:saveChangesRequest withContext:context mutableOperations:mutableOperations];
    
    // NSManagedObjectContext removes object references from an NSSaveChangesRequest as each object is saved, so create a copy of the original in order to send useful information in AFIncrementalStoreContextDidSaveRemoteValues notification.
    NSSaveChangesRequest *saveChangesRequestCopy = [[NSSaveChangesRequest alloc] initWithInsertedObjects:[saveChangesRequest.insertedObjects copy] updatedObjects:[saveChangesRequest.updatedObjects copy] deletedObjects:[saveChangesRequest.deletedObjects copy] lockedObjects:[saveChangesRequest.lockedObjects copy]];
    
    [self notifyManagedObjectContext:context aboutRequestOperations:mutableOperations forSaveChangesRequest:saveChangesRequestCopy];

    [self.HTTPClient enqueueBatchOfHTTPRequestOperations:mutableOperations progressBlock:nil completionBlock:^(NSArray *operations) {
        [self notifyManagedObjectContext:context aboutRequestOperations:operations forSaveChangesRequest:saveChangesRequestCopy];
    }];
    
    return [NSArray array];
}

#pragma mark - Expiring

- (void)expireObjectsWithIDs:(NSArray *)objectIDs context:(NSManagedObjectContext *)context
{
	dispatch_barrier_async(self.isolationQueue, ^{
		for (NSManagedObjectID *objectID in objectIDs) {
			[_expiredObjectIdentifiers addObject:objectIDs];
		}
	});
	
	NSManagedObjectContext *backingContext = [self backingManagedObjectContext];
	NSManagedObjectContext *childContext = [[NSManagedObjectContext alloc] initWithConcurrencyType:NSPrivateQueueConcurrencyType];
	childContext.parentContext = context;
	
	for (NSManagedObjectID *objectID in objectIDs) {
		__block NSString *resourceIdentifier = AFResourceIdentifierFromReferenceObject([self referenceObjectForObjectID:objectID]);

		[childContext performBlockAndWait:^{
			NSManagedObject *object = [childContext objectWithID:objectID];
			if (object) {
				[childContext deleteObject:object];
			}
		}];
		
		NSManagedObjectID *backingObjectID = [self objectIDForBackingObjectForEntity:[objectID entity] withResourceIdentifier:resourceIdentifier];
		
		[backingContext performBlockAndWait:^{
			NSManagedObject *backingObject = [_backingManagedObjectContext objectWithID:backingObjectID];
			if (backingObject) {
				[_backingManagedObjectContext deleteObject:backingObject];
			}
		}];
	}
	
	[backingContext performBlock:^{
		AFSaveManagedObjectContextOrThrowInternalConsistencyException(_backingManagedObjectContext);
	}];
	
	[childContext performBlock:^{
		AFSaveManagedObjectContextOrThrowInternalConsistencyException(childContext);
		[context performBlock:^{
			AFSaveManagedObjectContextOrThrowInternalConsistencyException(context);
		}];
	}];
	

}

#pragma mark - NSIncrementalStore

- (BOOL)loadMetadata:(NSError *__autoreleasing *)error {
    if (!_backingObjectIDByObjectID) {
        NSMutableDictionary *mutableMetadata = [NSMutableDictionary dictionary];
        [mutableMetadata setValue:[[NSProcessInfo processInfo] globallyUniqueString] forKey:NSStoreUUIDKey];
        [mutableMetadata setValue:NSStringFromClass([self class]) forKey:NSStoreTypeKey];
        [self setMetadata:mutableMetadata];
        
		NSString *label = [NSString stringWithFormat:@"%@.isolation.%p", [self class], self];
        self.isolationQueue = dispatch_queue_create([label UTF8String], 0);
		
        _backingObjectIDByObjectID = [[NSCache alloc] init];
        _registeredObjectIDsByEntityNameAndNestedResourceIdentifier = [[NSMutableDictionary alloc] init];
        _expiredObjectIdentifiers = [NSMutableSet set];
		
        NSManagedObjectModel *model = [self.persistentStoreCoordinator.managedObjectModel copy];
        for (NSEntityDescription *entity in model.entities) {
            // Don't add properties for sub-entities, as they already exist in the super-entity
            if ([entity superentity]) {
                continue;
            }
            
            NSAttributeDescription *resourceIdentifierProperty = [[NSAttributeDescription alloc] init];
            [resourceIdentifierProperty setName:kAFIncrementalStoreResourceIdentifierAttributeName];
            [resourceIdentifierProperty setAttributeType:NSStringAttributeType];
            [resourceIdentifierProperty setIndexed:YES];
            
            NSAttributeDescription *lastModifiedProperty = [[NSAttributeDescription alloc] init];
            [lastModifiedProperty setName:kAFIncrementalStoreLastModifiedAttributeName];
            [lastModifiedProperty setAttributeType:NSStringAttributeType];
            [lastModifiedProperty setIndexed:NO];
			
			NSAttributeDescription *etagProperty = [[NSAttributeDescription alloc] init];
            [etagProperty setName:kAFIncrementalStoreEtagAttributeName];
            [etagProperty setAttributeType:NSStringAttributeType];
            [etagProperty setIndexed:NO];

            
            [entity setProperties:[entity.properties arrayByAddingObjectsFromArray:[NSArray arrayWithObjects:resourceIdentifierProperty, lastModifiedProperty, etagProperty, nil]]];
        }
        
        _backingPersistentStoreCoordinator = [[NSPersistentStoreCoordinator alloc] initWithManagedObjectModel:model];
        
        return YES;
    } else {
        return NO;
    }
}

- (NSArray *)obtainPermanentIDsForObjects:(NSArray *)array error:(NSError **)error {
    NSMutableArray *mutablePermanentIDs = [NSMutableArray arrayWithCapacity:[array count]];
    for (NSManagedObject *managedObject in array) {
        NSManagedObjectID *managedObjectID = managedObject.objectID;
        if ([managedObjectID isTemporaryID] && managedObject.af_resourceIdentifier) {
            NSManagedObjectID *objectID = [self objectIDForEntity:managedObject.entity withResourceIdentifier:managedObject.af_resourceIdentifier];
            [mutablePermanentIDs addObject:objectID];
        } else {
            [mutablePermanentIDs addObject:managedObjectID];
        }
    }
    
    return mutablePermanentIDs;
}

- (id)executeRequest:(NSPersistentStoreRequest *)persistentStoreRequest
         withContext:(NSManagedObjectContext *)context
               error:(NSError *__autoreleasing *)error
{
    if (persistentStoreRequest.requestType == NSFetchRequestType) {
        return [self executeFetchRequest:(NSFetchRequest *)persistentStoreRequest withContext:context error:error];
    } else if (persistentStoreRequest.requestType == NSSaveRequestType) {
        return [self executeSaveChangesRequest:(NSSaveChangesRequest *)persistentStoreRequest withContext:context error:error];
    } else {
        NSMutableDictionary *mutableUserInfo = [NSMutableDictionary dictionary];
        [mutableUserInfo setValue:[NSString stringWithFormat:NSLocalizedString(@"Unsupported NSFetchRequestResultType, %d", nil), persistentStoreRequest.requestType] forKey:NSLocalizedDescriptionKey];
        if (error) {
            *error = [[NSError alloc] initWithDomain:AFNetworkingErrorDomain code:0 userInfo:mutableUserInfo];
        }
        
        return nil;
    }
}

- (NSIncrementalStoreNode *)newValuesForObjectWithID:(NSManagedObjectID *)objectID
                                         withContext:(NSManagedObjectContext *)context
                                               error:(NSError *__autoreleasing *)error
{
    NSFetchRequest *fetchRequest = [[NSFetchRequest alloc] initWithEntityName:[[objectID entity] name]];
    fetchRequest.resultType = NSDictionaryResultType;
    fetchRequest.fetchLimit = 1;
    fetchRequest.includesSubentities = NO;
    
    NSArray *attributes = [[[NSEntityDescription entityForName:fetchRequest.entityName inManagedObjectContext:context] attributesByName] allValues];
    NSArray *intransientAttributes = [attributes filteredArrayUsingPredicate:[NSPredicate predicateWithFormat:@"isTransient == NO"]];
    fetchRequest.propertiesToFetch = [[intransientAttributes valueForKeyPath:@"name"] arrayByAddingObjectsFromArray:@[kAFIncrementalStoreLastModifiedAttributeName, kAFIncrementalStoreEtagAttributeName]];
    
    fetchRequest.predicate = [NSPredicate predicateWithFormat:@"%K = %@", kAFIncrementalStoreResourceIdentifierAttributeName, AFResourceIdentifierFromReferenceObject([self referenceObjectForObjectID:objectID])];
    
    __block NSArray *results;
	__block NSDictionary *attributeValues = nil;
    NSManagedObjectContext *backingContext = [self backingManagedObjectContext];
    [backingContext performBlockAndWait:^{
        results = [backingContext executeFetchRequest:fetchRequest error:error];
		attributeValues = [results lastObject] ?: [NSDictionary dictionary];
    }];
    
    NSIncrementalStoreNode *node = [[NSIncrementalStoreNode alloc] initWithObjectID:objectID withValues:attributeValues version:1];
    
	if (_clientFlags.respondsToShouldFetchRemoteAttribute && [self.HTTPClient shouldFetchRemoteAttributeValuesForObjectWithID:objectID inManagedObjectContext:context]) {
		[self remoteFetchValuesForObjectWithID:objectID context:context attributeValues:attributeValues];
	}
	
    return node;
}

- (void)remoteFetchValuesForObjectWithID:(NSManagedObjectID *)objectID
								 context:(NSManagedObjectContext *)context
						 attributeValues:(NSDictionary *)attributeValues
{
	NSMutableURLRequest *request = [self.HTTPClient requestWithMethod:@"GET" pathForObjectWithID:objectID withContext:context];
	if (nil == request) {
		return;
	}
	
	// Setup metadata for request
	NSString *lastModified = [attributeValues objectForKey:kAFIncrementalStoreLastModifiedAttributeName];
	if (lastModified) {
		[request setValue:lastModified forHTTPHeaderField:@"Last-Modified"];
	}
	
	NSString *etag = [attributeValues objectForKey:kAFIncrementalStoreEtagAttributeName];
	if (etag) {
		[request setValue:etag forHTTPHeaderField:@"Etag"];
	}
	
	if ([attributeValues valueForKey:kAFIncrementalStoreLastModifiedAttributeName]) {
		[request setValue:[[attributeValues valueForKey:kAFIncrementalStoreLastModifiedAttributeName] description] forHTTPHeaderField:@"If-Modified-Since"];
	}
	
	if ([attributeValues valueForKey:kAFIncrementalStoreEtagAttributeName]) {
		[request setValue:[attributeValues valueForKey:kAFIncrementalStoreEtagAttributeName] forHTTPHeaderField:@"If-None-Match"];
	}
	
	NSManagedObjectContext *childContext = [[NSManagedObjectContext alloc] initWithConcurrencyType:NSPrivateQueueConcurrencyType];
	childContext.parentContext = context;
	childContext.mergePolicy = NSMergeByPropertyObjectTrumpMergePolicy;
	NSManagedObjectContext *backingContext = [self backingManagedObjectContext];
	
	AFHTTPRequestOperation *operation = [self.HTTPClient HTTPRequestOperationWithRequest:request success:^(AFHTTPRequestOperation *operation, NSDictionary *representation) {
		
		NSEntityDescription *entity = [objectID entity];
		NSString *resourceIdentifier = AFResourceIdentifierFromReferenceObject([self referenceObjectForObjectID:objectID]);
		
		// Do NOT mutate this inside a performBlock: call
		NSMutableDictionary *mutableAttributeValues = [attributeValues mutableCopy];
		[mutableAttributeValues addEntriesFromDictionary:[self.HTTPClient attributesForRepresentation:representation ofEntity:entity fromResponse:operation.response]];
		[mutableAttributeValues removeObjectForKey:kAFIncrementalStoreLastModifiedAttributeName];
		[mutableAttributeValues removeObjectForKey:kAFIncrementalStoreEtagAttributeName];
		
		[childContext performBlock:^{
			NSManagedObject *managedObject = [childContext existingObjectWithID:objectID error:nil];
			[managedObject setValuesForKeysWithDictionary:mutableAttributeValues];
			
			AFSaveManagedObjectContextOrThrowInternalConsistencyException(childContext);
			[self notifyManagedObjectContext:context aboutRequestOperation:operation forNewValuesForObjectWithID:objectID];
		}];
		
		[backingContext performBlock:^{
			NSManagedObjectID *backingObjectID = [self objectIDForBackingObjectForEntity:entity withResourceIdentifier:resourceIdentifier];
			NSManagedObject *backingObject = [[self backingManagedObjectContext] existingObjectWithID:backingObjectID error:nil];
			[backingObject setValuesForKeysWithDictionary:attributeValues];
			
			NSString *lastModified = [[operation.response allHeaderFields] valueForKey:@"Last-Modified"];
			if (lastModified) {
				[backingObject setValue:lastModified forKey:kAFIncrementalStoreLastModifiedAttributeName];
			}
			
			NSString *etag = [[operation.response allHeaderFields] valueForKey:@"Etag"];
			if (etag) {
				[backingObject setValue:etag forKey:kAFIncrementalStoreEtagAttributeName];
			}
			
			AFSaveManagedObjectContextOrThrowInternalConsistencyException(backingContext);
		}];
		
	} failure:^(AFHTTPRequestOperation *operation, NSError *error) {
		NSLog(@"Error: %@, %@", operation, error);
		[self notifyManagedObjectContext:context aboutRequestOperation:operation forNewValuesForObjectWithID:objectID];
	}];
	
	[self notifyManagedObjectContext:context aboutRequestOperation:operation forNewValuesForObjectWithID:objectID];
	[self.HTTPClient enqueueHTTPRequestOperation:operation];
}


- (id)newValueForRelationship:(NSRelationshipDescription *)relationship
              forObjectWithID:(NSManagedObjectID *)objectID
                  withContext:(NSManagedObjectContext *)context
                        error:(NSError *__autoreleasing *)error
{
	NSManagedObjectContext *backingContext = [self backingManagedObjectContext];
	
    if ([self.HTTPClient respondsToSelector:@selector(shouldFetchRemoteValuesForRelationship:forObjectWithID:inManagedObjectContext:)] && [self.HTTPClient shouldFetchRemoteValuesForRelationship:relationship forObjectWithID:objectID inManagedObjectContext:context]) {
        NSMutableURLRequest *request = [self.HTTPClient requestWithMethod:@"GET" pathForRelationship:relationship forObjectWithID:objectID withContext:context];
		
		// Check etag at destination object, don't attempt to set the etag values for to-many relationships
		NSManagedObject *object = [context existingObjectWithID:objectID error:nil];
		if (![object hasFaultForRelationshipNamed:[relationship name]] && ![relationship isToMany]) {
			NSManagedObject *destinationObject = [object valueForKey:[relationship name]];
			// Check etag
			NSString *etag = [destinationObject valueForKey:kAFIncrementalStoreEtagAttributeName];
			if (etag) {
				[request setValue:etag forHTTPHeaderField:@"Etag"];
			}
		}
        
        if ([request URL] && ![[context existingObjectWithID:objectID error:nil] hasChanges]) {
            NSManagedObjectContext *childContext = [[NSManagedObjectContext alloc] initWithConcurrencyType:NSPrivateQueueConcurrencyType];
            childContext.parentContext = context;
            childContext.mergePolicy = NSMergeByPropertyObjectTrumpMergePolicy;

            AFHTTPRequestOperation *operation = [self.HTTPClient HTTPRequestOperationWithRequest:request success:^(AFHTTPRequestOperation *operation, id responseObject) {

				NSString *resourceIdentifier = AFResourceIdentifierFromReferenceObject([self referenceObjectForObjectID:objectID]);
				NSManagedObjectID *backingObjectID = [self objectIDForBackingObjectForEntity:[objectID entity] withResourceIdentifier:resourceIdentifier];
				
				// Don't touch these outside a MOC block
				__block NSManagedObject *managedObject = nil;
				__block NSManagedObject *backingObject = nil;
				
				[childContext performBlockAndWait:^{
					managedObject = [childContext objectWithID:objectID];
				}];
				
				[backingContext performBlockAndWait:^{
					backingObject = (backingObjectID == nil) ? nil : [backingContext existingObjectWithID:backingObjectID error:nil];
				}];
				
				id representationOrArrayOfRepresentations = [self.HTTPClient representationOrArrayOfRepresentationsOfEntity:relationship.destinationEntity
																											forRelationship:relationship
																										 fromResponseObject:responseObject];
                
				[self insertOrUpdateObjectsFromRepresentations:representationOrArrayOfRepresentations ofEntity:relationship.destinationEntity fromResponse:operation.response withContext:childContext error:nil completionBlock:^(NSArray *managedObjectIDs, NSArray *backingObjectIDs) {
					
					[self updateRelationship:relationship
							forManagedObject:managedObject
								   inContext:childContext
							   backingObject:backingObject
							   withObjectIDs:managedObjectIDs
							backingObjectIDs:backingObjectIDs];
					
					[childContext performBlockAndWait:^{
						AFSaveManagedObjectContextOrThrowInternalConsistencyException(childContext);
					}];
					
					[backingContext performBlockAndWait:^{
						AFSaveManagedObjectContextOrThrowInternalConsistencyException(backingContext);
					}];
					
					[self notifyManagedObjectContext:context aboutRequestOperation:operation forNewValuesForRelationship:relationship forObjectWithID:objectID];
				}];
            } failure:^(AFHTTPRequestOperation *operation, NSError *error) {
                NSLog(@"Error: %@, %@", operation, error);
                [self notifyManagedObjectContext:context aboutRequestOperation:operation forNewValuesForRelationship:relationship forObjectWithID:objectID];
            }];
			
			[self notifyManagedObjectContext:context aboutRequestOperation:operation forNewValuesForRelationship:relationship forObjectWithID:objectID];
            [self.HTTPClient enqueueHTTPRequestOperation:operation];
        }
    }

    
	NSString *resourceIdentifier = AFResourceIdentifierFromReferenceObject([self referenceObjectForObjectID:objectID]);
	NSManagedObjectID *backingObjectID = [self objectIDForBackingObjectForEntity:[objectID entity] withResourceIdentifier:resourceIdentifier];
	
	__block id returnValue = nil;
	
	[backingContext performBlock:^{
		NSManagedObject *backingObject = (backingObjectID == nil) ? nil : [backingContext existingObjectWithID:backingObjectID error:nil];
		if (!backingObject) {
			return;
		}
		
		id backingRelationshipObject = [backingObject valueForKeyPath:relationship.name];
		if ([relationship isToMany]) {
			NSMutableArray *mutableObjects = [NSMutableArray arrayWithCapacity:[backingRelationshipObject count]];
			for (NSString *resourceIdentifier in [backingRelationshipObject valueForKeyPath:kAFIncrementalStoreResourceIdentifierAttributeName]) {
				NSManagedObjectID *objectID = [self objectIDForEntity:relationship.destinationEntity withResourceIdentifier:resourceIdentifier];
				[mutableObjects addObject:objectID];
			}
			
			returnValue = mutableObjects;
		} else {
			NSString *resourceIdentifier = [backingRelationshipObject valueForKeyPath:kAFIncrementalStoreResourceIdentifierAttributeName];
			NSManagedObjectID *objectID = [self objectIDForEntity:relationship.destinationEntity withResourceIdentifier:resourceIdentifier];
			
			returnValue = objectID ?: [NSNull null];
		}
	}];
    
	if (nil == returnValue) {
		if ([relationship isToMany]) {
            returnValue = [NSArray array];
        } else {
            returnValue = [NSNull null];
        }
	}
	
	return returnValue;
}

- (void)managedObjectContextDidRegisterObjectsWithIDs:(NSArray *)objectIDs {
    [super managedObjectContextDidRegisterObjectsWithIDs:objectIDs];
    
		for (NSManagedObjectID *objectID in objectIDs) {
			
			id referenceObject = [self referenceObjectForObjectID:objectID];
			if (!referenceObject) {
				continue;
			}
						
			dispatch_barrier_async(self.isolationQueue, ^{
				NSMutableDictionary *objectIDsByResourceIdentifier = [_registeredObjectIDsByEntityNameAndNestedResourceIdentifier objectForKey:objectID.entity.name] ?: [NSMutableDictionary dictionary];
				[objectIDsByResourceIdentifier setObject:objectID forKey:AFResourceIdentifierFromReferenceObject(referenceObject)];
				
				[_registeredObjectIDsByEntityNameAndNestedResourceIdentifier setObject:objectIDsByResourceIdentifier forKey:objectID.entity.name];
			});
		}
}

- (void)managedObjectContextDidUnregisterObjectsWithIDs:(NSArray *)objectIDs {
    [super managedObjectContextDidUnregisterObjectsWithIDs:objectIDs];
    
	dispatch_barrier_async(self.isolationQueue, ^{
		for (NSManagedObjectID *objectID in objectIDs) {
			[[_registeredObjectIDsByEntityNameAndNestedResourceIdentifier objectForKey:objectID.entity.name] removeObjectForKey:AFResourceIdentifierFromReferenceObject([self referenceObjectForObjectID:objectID])];
		}
	});
}

@end
