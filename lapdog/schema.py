from firecloud import api
import json

fc_data_types = {
    'sample',
    'sample_set',
    'participant',
    'participant_set',
    'pair',
    'pair_set'
}

class EvaluationError(ValueError):
    pass

def determine_reference_type(ref):
    return ref.rstrip('s_').lower()

class AttrArray(list):
    def __getattr__(self, attr):
        try:
            return AttrArray([
                getattr(item, attr)
                for item in self
            ])
        except AttributeError as e:
            raise AttributeError("One or more items in this array do not have attribute '%s'" % attr) from e

    def __call__(self, *args, **kwargs):
        try:
            return AttrArray([
                item(*args, **kwargs)
                for item in self
            ])
        except TypeError as e:
            raise TypeError("One or more items in this array are not callable") from e

class Evaluator(object):
    def __init__(self, entities):
        """
        Takes an entities dict (/api/workspaces/{}/{}/entities)
        This defines the available entity types and what can be interpreted as a reference
        """
        self.entities = entities
        self.edata = {}
        self.attributes = {}
        self.ids = {
            data['idName']:typ
            for typ, data in entities.items()
        }

    def add_entities(self, etype, df):
        self.edata[etype] = df

    def add_attributes(self, attrs):
        self.attributes = attrs

    def __call__(self, etype, entity, expression):
        components = expression.split('.')
        if components[0] == 'workspace':
            if components[1] not in self.attributes:
                raise EvaluationError("No workspace attribute '%s' : %s" % (
                    components[1],
                    expression
                ))
            return [self.attributes[components[1]]]
        elif components[0] == 'this':
            if etype not in self.edata:
                raise EvaluationError("Entity type not loaded '%s' : %s" % (
                    etype,
                    expression
                ))
            if entity not in self.edata[etype].index:
                raise EvaluationError("No such %s '%s' : %s" % (
                    etype,
                    entity,
                    expression
                ))
            this = self.edata[etype].loc[entity]
            for key in components[1:-1]:
                # print(this)
                _etype = determine_reference_type(key)
                if _etype not in fc_data_types and _etype not in self.edata:
                    raise EvaluationError("No such entity type '%s' : %s" % (
                        _etype,
                        expression
                    ))
                elif _etype not in self.edata:
                    raise EvaluationError("Entity type not loaded '%s' : %s" % (
                        _etype,
                        expression
                    ))
                try:
                    #FIXME: Prolly need to check if 'this' is singular or not
                    this = self.edata[_etype].loc[[*getattr(this, key)]]
                except Exception as e:
                    raise EvaluationError("Unable to evaluate expression reference '%s' : %s" % (
                        key,
                        expression
                    )) from e
            try:
                if components[-1] in self.ids:
                    if len(components) <= 2:
                        return [this.name]
                    return [*this.index]
                else:
                    if len(components) <= 2:
                        val = getattr(this, components[-1])
                        if not isinstance(val, list):
                            return [val]
                        return [*val]
                    return [*getattr(this, components[-1])]
            except Exception as e:
                raise EvaluationError("Unable to evaluate expression attribute '%s' : %s" % (
                    components[-1],
                    expression
                )) from e
        try:
            return [json.loads(expression)]
        except Exception as e:
            raise EvaluationError("Invalid expression: %s" % expression) from e
