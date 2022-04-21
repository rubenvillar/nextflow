package nextflow.script.dsl

import groovy.transform.CompileDynamic
import nextflow.script.IncludeDef

/**
 * @author : jorge <jorge.aguilera@seqera.io>
 *
 */
class ChannelExtensionSpec {

    List<MethodSpec> methodSpecs = []

    //get
    def propertyMissing(String name) {
        MethodSpec methodSpec = new MethodSpec(name:name, alias:name)
        methodSpecs.add methodSpec
        this
    }

    MethodSpec alias(ChannelExtensionSpec me){
        me.methodSpecs.last()
    }

    ChannelExtensionSpec from( String plugin ){
        assert methodSpecs.size(), "Missing methods to import from $plugin extension"
        ChannelExtensionLoader loader = new ChannelExtensionLoader()
        loader.load(plugin, methodSpecs as Set<MethodSpec>)
        this
    }

    class MethodSpec{
        String name
        String alias

        MethodSpec 'as'(String str){
            this.alias = str
            this
        }
    }
}
