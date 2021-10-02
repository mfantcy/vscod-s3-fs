import { fromIni } from "@aws-sdk/credential-provider-ini";
import { Credentials, Provider } from "@aws-sdk/types";
import { IConfig } from "./config";

export const provideCredentials = (config: IConfig): Credentials | Provider<Credentials> | undefined => {
    if (config.awsProfile || config.awsConfigFile || config.awsSharedCredentialsFile) {
        return fromIni({ profile: config.awsProfile, filepath: config.awsSharedCredentialsFile, configFilepath: config.awsConfigFile });
    }
    return undefined;
};
