import {
    CopyObjectCommand, DeleteObjectCommand, DeleteObjectsCommand,
    GetObjectCommand,
    ListObjectsCommand,
    ListObjectsCommandOutput,
    PutObjectCommand,
    PutObjectCommandOutput,
    S3Client,
    S3ClientResolvedConfig,
    ServiceInputTypes,
    ServiceOutputTypes
} from "@aws-sdk/client-s3";
import { isThrottlingError } from "@aws-sdk/service-error-classification";
import { Command } from "@aws-sdk/types";
import { Readable } from "stream";
import { Uri } from "vscode";

export class S3ClientWrapper {
    private cachesAndResolvers: {
        [id: string]: {
            cached?: { res: any, err: any, isError: boolean, expiry: number }
            resolvers?: { rsv: (r: any) => void, rej: (err: any) => void }[]
        }
    };
    constructor(private readonly s3Client: S3Client, public defaultCacheTtl: number = 2000) {
        this.cachesAndResolvers = {};
    }
    async listObjects(uri: Uri, options: { limit?: number, findMatch?: boolean, cacheTtl?: number } = {}): Promise<ListObjectsCommandOutput> {
        const { limit, findMatch, cacheTtl } = {
            limit: undefined,
            findMatch: undefined,
            cacheTtl: this.defaultCacheTtl,
            ...options
        };
        const cacheKey = this.createCacheKey(uri, 'listObjects', `${limit}-${!!findMatch}`);
        const bucket = uri.authority;
        const prefix = this.normalizedS3Key(uri.path);
        return this.cachedOrPromise(cacheKey, async (): Promise<ListObjectsCommandOutput> => {
            // eslint-disable-next-line @typescript-eslint/naming-convention
            let result: ListObjectsCommandOutput = { $metadata: {}, Contents: [], CommonPrefixes: [], NextMarker: '' };
            const handler = async (r: ListObjectsCommandOutput): Promise<ListObjectsCommandOutput> => {
                r.Contents = result.Contents?.concat(r.Contents || []);
                r.CommonPrefixes = result.CommonPrefixes?.concat(r.CommonPrefixes || []);
                if (findMatch) {
                    const filterF = (k: string) => ((n: any) => n[k] && n[k] === prefix || (!prefix.endsWith('/') && n[k].startsWith(`${prefix}/`)));
                    r.Contents = r.Contents!.filter(filterF('Key'));
                    r.CommonPrefixes = r.CommonPrefixes!.filter(filterF('Prefix'));
                }
                result = r;
                if (
                    r.NextMarker === undefined
                    || (findMatch && result.Contents!.length > 0 || r.CommonPrefixes!.length > 0)
                    || (limit && limit <= result.Contents!.length + r.CommonPrefixes!.length)
                ) {
                    return result;
                }
                return this.sendAndHandleThrottling(
                    // eslint-disable-next-line @typescript-eslint/naming-convention
                    new ListObjectsCommand({ Bucket: bucket, Prefix: prefix, Delimiter: '/', MaxKeys: limit, Marker: r.NextMarker || undefined }),
                    { tries: 3, retryAfterMs: 500 }
                ).then(handler);
            };
            return handler(result);
        }, cacheTtl);
    }
    async getObject(uri: Uri, cacheTtl: number = this.defaultCacheTtl): Promise<Uint8Array> {
        const cacheKey = this.createCacheKey(uri, 'getObject');
        const bucket = uri.authority;
        const key = this.normalizedS3Key(uri.path);
        return this.cachedOrPromise(cacheKey, async (): Promise<Uint8Array> => {
            // eslint-disable-next-line @typescript-eslint/naming-convention
            return this.sendAndHandleThrottling(new GetObjectCommand({ Bucket: bucket, Key: key }), { tries: 3, retryAfterMs: 500 })
                .then(output => {
                    if (!output.Body) {
                        return new Uint8Array();
                    } else if (typeof Blob !== 'undefined' && output.Body instanceof Blob) {
                        return dataFromBlob(output.Body);
                    } else if (typeof ReadableStream !== 'undefined' && output.Body instanceof ReadableStream) {
                        return dataFromReadableStream(output.Body);
                    }
                    return dataFromReadable(output.Body as Readable);
                });
        }, cacheTtl);
    }

    async putObject(uri: Uri, content: Uint8Array): Promise<PutObjectCommandOutput> {
        const bucket = uri.authority;
        const key = this.normalizedS3Key(uri.path);
        // eslint-disable-next-line @typescript-eslint/naming-convention
        const res = this.sendAndHandleThrottling(new PutObjectCommand({ Bucket: bucket, Key: key, Body: content }), { tries: 2, retryAfterMs: 500 });
        await this.cleanCache(uri);
        return res;
    }

    async createFolder(uri: Uri): Promise<void> {
        const bucket = uri.authority;
        let key = this.normalizedS3Key(uri.path);
        if (!key.endsWith('/')) {
            key += '/';
        }
        console.log('create folder', bucket, key);
        // eslint-disable-next-line @typescript-eslint/naming-convention
        await this.sendAndHandleThrottling(new PutObjectCommand({ Bucket: bucket, Key: key }), { tries: 2, retryAfterMs: 500 });
        await this.cleanCache(uri.toString().replace(/\/[^\/]+\/?$/, ''));
    }

    async deleteObject(uri: Uri): Promise<void> {
        const bucket = uri.authority;
        const key = this.normalizedS3Key(uri.path);
        // eslint-disable-next-line @typescript-eslint/naming-convention
        await this.sendAndHandleThrottling(new DeleteObjectCommand({ Bucket: bucket, Key: key }), { tries: 2, retryAfterMs: 500 });
        await this.cleanCache(uri.toString().replace(/\/[^\/]+\/?$/, ''));
    }

    async deleteFolder(uri: Uri): Promise<void> {
        await this.cleanCache(uri);
        const bucket = uri.authority;
        const listOutput = await this.listObjects(uri, { cacheTtl: 0 });
        if (listOutput.Contents) {
            const limit = 500;
            const pSize = Math.ceil(listOutput.Contents.length / limit);
            for (let i = 0; i < pSize; i++) {
                const offset = i * limit;
                const keys = listOutput.Contents.slice(offset, offset + limit)
                    .map(c => {
                        // eslint-disable-next-line @typescript-eslint/naming-convention
                        return c.Key ? { Key: c.Key } : undefined;
                    })
                    // eslint-disable-next-line @typescript-eslint/naming-convention
                    .filter(k => !!k) as { Key: string }[];
                // eslint-disable-next-line @typescript-eslint/naming-convention
                await this.s3Client.send(new DeleteObjectsCommand({ Bucket: bucket, Delete: { Objects: keys } }));
            }
        }
        if (listOutput.CommonPrefixes) {
            for (let i = 0; i < listOutput.CommonPrefixes.length; i++) {
                if (listOutput.CommonPrefixes[i].Prefix) {
                    const folderUri = Uri.from({ ...uri.toJSON(), path: `/${listOutput.CommonPrefixes[i].Prefix}` });
                    await this.deleteFolder(folderUri);
                }
            }
        }

        await this.cleanCache(uri.toString().replace(/\/[^\/]+\/?$/, ''));
    }

    async copyObject(sourceUri: Uri, destinationUri: Uri, deleteSource: boolean = false): Promise<void> {
        const destinationBucket = destinationUri.authority;
        const destinationKey = this.normalizedS3Key(destinationUri.path);
        const copySource = `/${sourceUri.authority}${sourceUri.path}`;
        console.log(copySource);
        await this.sendAndHandleThrottling(
            // eslint-disable-next-line @typescript-eslint/naming-convention
            new CopyObjectCommand({ CopySource: copySource, Bucket: destinationBucket, Key: destinationKey }),
            { tries: 2, retryAfterMs: 500 }
        );
        const cleanPromise = [this.cleanCache(destinationUri.toString().replace(/\/[^\/]+\/?$/, ''))];
        if (deleteSource) {
            await this.deleteObject(sourceUri);
            cleanPromise.push(this.cleanCache(sourceUri.toString().replace(/\/[^\/]+\/?$/, '')));
        }
        await Promise.all(cleanPromise);
    }

    async copyFolder(sourceUri: Uri, destinationUri: Uri, deleteSource: boolean = false): Promise<void> {
        sourceUri = sourceUri.path.endsWith('/') ? sourceUri : Uri.from({ ...sourceUri.toJSON(), path: sourceUri.path + '/' });
        destinationUri = destinationUri.path.endsWith('/') ? destinationUri : Uri.from({ ...destinationUri.toJSON(), path: destinationUri.path + '/' });
        const listOutput = await this.listObjects(sourceUri);
        const parallelSize = 10;
        const delFunc = async (method: (s: Uri, d: Uri, del: boolean) => Promise<any>, c: any[], k: string) => {
            const p = Math.ceil(c.length / parallelSize);
            for (let i = 0; i < p; i++) {
                const offset = i * parallelSize;
                const promises = c.slice(offset, offset + parallelSize)
                    .map(c => {
                        if (!c[k]) {
                            return undefined;
                        }
                        const fileName = c[k].substring(sourceUri.path.length - 1);
                        return method.call(
                            this,
                            Uri.from({ ...sourceUri.toJSON(), path: sourceUri.path + fileName }),
                            Uri.from({ ...destinationUri.toJSON(), path: destinationUri.path + fileName }),
                            deleteSource
                        );
                    })
                    .filter(u => !!u) as Promise<any>[];

                await Promise.all(promises);
            }
        };
        if (listOutput.Contents) {
            await delFunc(this.copyObject, listOutput.Contents, 'Key');
        }
        if (listOutput.CommonPrefixes) {
            await delFunc(this.copyFolder, listOutput.CommonPrefixes, 'Prefix');
        }
        await this.cleanCache(destinationUri.toString().replace(/\/[^\/]+\/?$/, ''));
        const cleanPromise = [this.cleanCache(destinationUri.toString().replace(/\/[^\/]+\/?$/, ''))];
        if (deleteSource) {
            cleanPromise.push(this.cleanCache(sourceUri.toString().replace(/\/[^\/]+\/?$/, '')));
        }
        await Promise.all(cleanPromise);
    }

    normalizedS3Key(path: string): string {
        return path.trim().replace(/^\/+/, '');
    }

    private async cleanCache(uri: Uri | string): Promise<void> {
        console.debug(`cleanCache ${uri}`);
        const uriStr = uri.toString();
        const ps: Promise<void>[] = [];
        Object.keys(this.cachesAndResolvers)
            .forEach(cacheKey => {
                if (!cacheKey.startsWith(uriStr)) { return; }
                const cacheAndResolver = this.cachesAndResolvers[cacheKey];
                if (cacheAndResolver?.resolvers) {
                    ps.push(new Promise(r => {
                        const rsv = () => { delete this.cachesAndResolvers[cacheKey]; r(); };
                        cacheAndResolver.resolvers?.push({ rsv, rej: rsv });
                    }));
                } else {
                    delete this.cachesAndResolvers[cacheKey];
                }
            });
        return Promise.all(ps).then(() => { });
    }

    private createCacheKey(uri: Uri, method: string, addition?: any): string {
        return addition ? `${uri.toString()}-${method}-${addition.toString()}` : `${uri.toString()}-${method}`;
    }

    private cachedOrPromise<T>(key: string, promiseFunc: () => Promise<T>, cacheTtl: number): Promise<T> {
        const cacheAndR = this.cachesAndResolvers[key];
        const cached = cacheAndR?.cached;
        if (cached) {
            if (cached.expiry > Date.now()) {
                return new Promise((res, rej) => {
                    return cached.isError ? rej(cached.err) : res(cached.res);
                });
            }
            if (cacheAndR.resolvers) {
                delete cacheAndR.cached;
            } else {
                delete this.cachesAndResolvers[key];
            }
        }
        return new Promise<T>((rsv, rej) => {
            const cacheAndResolver = this.cachesAndResolvers[key] || (this.cachesAndResolvers[key] = {});
            if (cacheAndResolver?.resolvers) {
                cacheAndResolver.resolvers.push({ rsv, rej });
                return;
            }
            cacheAndResolver.resolvers = [{ rsv, rej }];
            const revFunc = (r: 'rsv' | 'rej') => {
                return (result: any) => {
                    const isError = r === 'rej';
                    const expiry = Date.now() + cacheTtl;
                    cacheAndResolver.cached = {
                        res: !isError ? result : undefined,
                        err: isError ? result : undefined,
                        isError,
                        expiry
                    };
                    cacheAndResolver.resolvers!.forEach(resolver => {
                        resolver[r](result);
                    });
                    delete cacheAndResolver.resolvers;
                };
            };
            promiseFunc().then(revFunc('rsv')).catch(revFunc('rej'));
        });
    }

    private async sendAndHandleThrottling<InputType extends ServiceInputTypes, OutputType extends ServiceOutputTypes>(
        command: Command<ServiceInputTypes, InputType, ServiceOutputTypes, OutputType, S3ClientResolvedConfig>,
        throttlingOptions: { tries: number, retryAfterMs: number }
    ): Promise<OutputType> {
        const { tries, retryAfterMs } = throttlingOptions;
        return this.s3Client.send(command).catch(async e => {
            if (!isThrottlingError(e) || tries <= 0) {
                return Promise.reject(e);
            }
            console.warn(`retry ${command.constructor.name} ${JSON.stringify(command)}`);
            return new Promise((rsv, rej) => {
                setTimeout(
                    () => this.sendAndHandleThrottling(command, { tries: tries - 1, retryAfterMs }).then(rsv).catch(rej),
                    retryAfterMs
                );
            });
        });
    }
}

const dataFromReadable = async (readable: Readable): Promise<Uint8Array> => {
    return new Promise<Uint8Array>((rsv, rej) => {
        const chunks: Buffer[] = [];
        readable.on('data', (chunk) => {
            chunks.push(chunk);
        });
        readable.on('end', () => {
            rsv(Buffer.concat(chunks));
        });
        readable.on('error', rej);
    });
};

const dataFromReadableStream = async (readableStream: ReadableStream): Promise<Uint8Array> => {
    const reader = readableStream.getReader();

    const readHandler = (result: ReadableStreamDefaultReadResult<any>): Promise<Uint8Array> | Uint8Array => {
        const chunks: Uint8Array[] = [];
        if (result.done) {
            return Buffer.concat(chunks);
        }
        if (typeof result.value === 'string') {
            chunks.push((new TextEncoder()).encode(result.value));
        } else if (result.value instanceof Uint8Array) {
            chunks.push(result.value);
        } else {
            const data: Array<number> = Array.from(result.value);
            chunks.push(Uint8Array.from(data));
        }
        return reader.read().then(readHandler);
    };
    return reader.read().then(readHandler);
};

const dataFromBlob = async (blob: Blob): Promise<Uint8Array> => {
    return blob.arrayBuffer().then(b => new Uint8Array(b));
};