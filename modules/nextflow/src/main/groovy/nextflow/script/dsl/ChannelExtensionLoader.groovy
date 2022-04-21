package nextflow.script.dsl

import groovy.util.logging.Slf4j
import nextflow.extension.ChannelExtensionDelegate
import nextflow.plugin.Plugins


/**
 * @author : jorge <jorge.aguilera@seqera.io>
 *
 */
@Slf4j
class ChannelExtensionLoader {

    void load( String pluginId, Set<ChannelExtensionSpec.MethodSpec> methods){
        log.info "starting plugin $pluginId"
        Plugins.startIfMissing(pluginId)
        Map<String, String> alias = methods.collectEntries {[it.name,it.alias]}
        ChannelExtensionDelegate.INSTANCE().loadChannelExtensionInPlugin(pluginId, alias)
    }

}
